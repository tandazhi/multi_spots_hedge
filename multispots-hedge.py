# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import logging
from logging.handlers import RotatingFileHandler
from retrying import retry
from datetime import datetime
import math
import ccxt
import config

# logging.basicConfig(level=logging.DEBUG,
# 	format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
# 	datefmt='%a, %d %b %Y %H:%M:%S',
# 	filename='myapp.log',
# 	filemode='w')

#从config文件生产myExchange类， 包含ccxt类指针， apikey，交易费率， 初始balance等信息. 不支持okex期货，因为期货的账户权益与现货不同
class myExchange(object):
	@retry
	def __init__(self,market):
		self.id=market['id']
		exec('self.exchange=ccxt.'+self.id+'()')
		# self.future=False
		# if self.id=='okex' and (market['symbol'] in ('BTC/USD','LTC/USD')):
		# 	self.future=True
		self.exchange.apiKey=market['api_key']
		self.exchange.secret=market['sec_token']
		self.symbol=market['symbol']
		self.feeTaker=market['feeTaker']
		self.feeMaker=market['feeMaker']
		self.depth={} #store the orderbook data
		self.balance={} #store the balance info of the account


	@retry
	def getBalance(self):
		currencyPair=re.split(r'/', self.symbol)
		# if self.future:
		# 	balance=self.exchange.private_post_future_userinfo()
		# else:
		balance=self.exchange.fetchBalance()
		# logging.debug(balance)

		if balance:
			self.balance={'stocks':balance[currencyPair[0]],'balance':balance[currencyPair[1]]}
			return self.balance
		
		logging.error('%s getBalance return no data!!!', market.id)
		return
#----------------------------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------
class hedge(object):

	def __init__(self):
		self.exchanges=[]
		self.initTotalBalance={}
		self.currentTotalBalance={}
		# self.currencyPair=[]
		self.lastProfit=0
		self.lastOpAmount=0
		self.averagePrice=0
		self.isBalance=True #if the spot position is balance
		self.isNormal=True #if the spot position is normal

	def init_logger(self):
		screenLevel = logging.INFO
		logLevel = logging.INFO
		logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S',level=screenLevel)
		Rthandler = RotatingFileHandler('hedge.log', maxBytes=100*1024*1024,backupCount=10)
		Rthandler.setLevel(logLevel)
		formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')  
		Rthandler.setFormatter(formatter)
		logging.getLogger('').addHandler(Rthandler)
		return

	#to initialized the exchanges and running parameters
	def initExchanges(self,config):
		logging.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
		self.exchanges=[]

		if len(config.markets)<2:
			logging.error("Must have more than two markets to start the hedge!!!")
			sys.exit()
		for market in config.markets:
			try:
				self.exchanges.append(myExchange(market))
			except Exception as e:
				logging.error(e)

		self.interval=max(config.interval,300)  #出错重试间隔（毫秒）
		self.minDiff=config.minDiff #最低差价百分比（%）
		self.slideP=config.slideP #滑动价百分比(%)
		self.stopPL=config.stopPL #跌停值, 价格异常
		self.stopPH=config.stopPH #涨停值，价格异常
		self.minAmount=max(config.minAmount,0.1) #单笔最小交易数量
		self.maxAmount=min(config.maxAmount,5) #单笔最大交易数量
		self.useMarketOrder=config.useMarketOrder #是否使用市价单止损?
		self.stop_when_loss=config.stop_when_loss #亏损时停止?
		self.max_loss=config.max_loss #最大亏损额
		self.maxLagTime=config.maxLagTime #各交易所间行情间隔最大时间, 秒

		return

	#获取各交易所balance信息
	def getAllBalance(self):
		allBalance={}

		for p in self.exchanges:
			try:
				starttime=int(time.time()*1000)
				logging.debug('fetch start: %d', starttime)
				singleBalance=p.getBalance()
				endtime=int(time.time()*1000)
				logging.debug('fetch end: %d; Lag time: %d ms',endtime, (endtime-starttime))
				logging.debug('%s balance: %s',p.id.upper(), singleBalance)

				for key in singleBalance.keys():
					if key in allBalance:
						allBalance[key]['free']+=singleBalance[key]['free']
						allBalance[key]['used']+=singleBalance[key]['used']
						allBalance[key]['total']+=singleBalance[key]['total']
					else:
						allBalance[key]=singleBalance[key]

			except Exception as e:
				logging.error(e)

		logging.debug('Total Balance: %s', allBalance)
		return allBalance

	#获取各交易所交易对深度信息
	def _getAllDepth(self):
		for p in self.exchanges:
			try:
				p.depth=p.exchange.fetchOrderBook(p.symbol,{'size':20,'depth':20})
				logging.debug('Exchange %s order Book: %s', p.id,p.depth)
			except Exception as e:
				logging.error("%s getOrderBook Error: %s",p.id,e)
		return

	#执行此函数前， 需要先更新交易所账户信息getAllBalance，获取账户balance， 并且更新账户交易所深度信息getAllDepth， 获取交易所orderbook
	def getMaxSpread(self): 

		self._getAllDepth()
		i=0
		for p in self.exchanges:
			if p.depth:
				i+=1
		if i<2:
			logging.error("Less than two exchanges have order book data, can't hedge......")
			return

		maxBid=0
		maxBidAmount=0
		maxPair=None
		minAsk=1000
		minAskAmount=0
		minPair=None
		for p in self.exchanges:
			if p.depth:
				if maxBid<p.depth['bids'][1][0] and min(p.balance['stocks']['free'],p.depth['bids'][1][1])>=self.minAmount: #跳过盘口买一价, 且该交易所有足够币交易
					maxBid= p.depth['bids'][1][0]
					maxBidAmount=p.depth['bids'][1][1]
					maxPair=p
				if minAsk>p.depth['asks'][1][0] and p.depth['asks'][1][1]>=self.minAmount and self._floatFloor(p.balance['balance']['free'],6)>p.depth['asks'][1][0]*self.minAmount*(1+p.feeTaker): #跳过盘口买一价, 且该交易所有足够钱交易
					minAsk=p.depth['asks'][1][0]
					minAskAmount=p.depth['asks'][1][1]
					minPair=p
		if minPair and maxPair:
			logging.info("max bid exchange:%s, max bid price:%s, max bid amount:%s; min ask exchange:%s, min ask price:%s, min ask amount:%s, spread percent:%f",maxPair.id,maxBid,maxBidAmount,minPair.id,minAsk,minAskAmount, (maxBid/minAsk-1)*100)
			self.averagePrice=(maxPair.depth['bids'][0][0]+minPair.depth['bids'][0][0])/2
		return maxBid,maxBidAmount,maxPair,minAsk,minAskAmount,minPair

	def doHedge(self):

		maxBid,maxBidAmount,maxPair,minAsk,minAskAmount,minPair=self.getMaxSpread()

		if (maxPair==None) or (minPair==None):  #未筛选出对冲交易所， 返回
			return


		if maxBidAmount==0 or minAskAmount==0 or (maxPair.id==minPair.id) or (maxBid<minAsk): #未筛选出对冲交易所， 返回
			return

		if (not self._isPriceNormal(maxBid)) or (not self._isPriceNormal(minAsk)): #价格不正常， 返回
			logging.warning('The price is unormal, jump over this cycle......')
			return

		if abs(maxPair.depth['timestamp']-minPair.depth['timestamp'])>self.maxLagTime*1000 or abs(time.time()*1000-maxPair.depth['timestamp'])>self.maxLagTime*1000: #深度信息时间差大于阈值则不操作
			logging.warning('the time lag between different exchanges is bigger than 3 seconds！！！！！')
			return
		
		# orderbook 价格不正常， 返回
		if maxPair.depth['asks'][0][0]<maxPair.depth['bids'][0][0] or maxPair.depth['asks'][0][0]>maxPair.depth['asks'][1][0] \
			or minPair.depth['asks'][0][0]<minPair.depth['bids'][0][0] or minPair.depth['asks'][0][0]>minPair.depth['asks'][1][0]:
			logging.warning('Depth info is unormal, jump over this cycle......')
			return

		if maxBid>=minAsk*(1+self.minDiff/100): #价差超过对冲阈值，则执行如下对冲操作
			canSellAmount=min(maxPair.balance['stocks']['free'],maxBidAmount,self.maxAmount)
			canBuyAmount=minPair.balance['balance']['free']/minAsk/(1+self.slideP/100)/(1+minPair.feeTaker)
			hedgeAmount=self._floatFloor(min(canSellAmount,canBuyAmount),3) #对冲币数量取3位小数
			self.lastOpAmount=hedgeAmount #记录每次的操作币数量
			logging.info('Exchange %s buy %f coins; Exchange %s sell %f coins.', minPair.id,hedgeAmount,maxPair.id,hedgeAmount)
			if(maxPair.exchange.createLimitSellOrder(maxPair.symbol,hedgeAmount,self._floatFloor(maxBid*(1-self.slideP/100),8))):
				minPair.exchange.createLimitBuyOrder(minPair.symbol,hedgeAmount,self._floatCeil(minAsk*(1+self.slideP/100),8))
			self.isBalance=False

		return

	def filter_orders_by_status(self, orders, status):
		result = []
		for i in range(0, len(orders)):
		    if orders[i]['status'] == status:
		        result.append(orders[i])
		return result


	def _isPriceNormal(self, price):
		if price>self.stopPH or price<self.stopPL:
			return False
		return True

	def _floatFloor(self, number, decimal=8):
		return math.floor(number*10**decimal)/10**decimal

	def _floatCeil(self, number, decimal=8):
		return math.ceil(number*10**decimal)/10**decimal


	def doBalance(self):
		self._cancelAllOrder()
		self.currentTotalBalance=self.getAllBalance()
		stockDiff=self._floatFloor((self.currentTotalBalance['stocks']['total']-self.initTotalBalance['stocks']['total']),3)
		if abs(stockDiff)>1.1*self.maxAmount:
			self.isNormal=False
			logging.warning("Errors: 仓位变动异常! 停止交易")
			sys.exit()

		if abs(stockDiff)<self.minAmount:
			self.isBalance=True

		else:
			orderAccount=0
			Logging.info('初始币总数量:%s; 现在币总数量:%s; 差额:%s',self.initTotalBalance['stocks']['total'], self.currentTotalBalance['stocks']['total'],stockDiff)
			maxBid,maxBidAmount,maxPair,minAsk,minAskAmount,minPair=self.getMaxSpread()
			self.lastOpAmount=stockDiff
			if stockDiff>0:
				maxPair.exchange.createLimitSellOrder(maxPair.symbol,stockDiff,self._floatFloor(maxBid*(1-self.slideP/100),8))
			else:
				minPair.exchange.createLimitBuyOrder(minPair.symbol,stockDiff,self._floatCeil(minAsk*(1+self.slideP/100),8))
		if self.isBalance and self.lastOpAmount:
			currentProfit=self.getProfit()
			logging.info('Profit: %s; Spread: %s; Balance: %s, Stocks: %s.',currentProfit,(currentProfit-self.lastProfit)/self.lastOpAmount, \
				self.currentTotalBalance['balance']['total'],self.currentTotalBalance['stocks']['total'])
			if self.stop_when_loss and currentProfit<0 and abs(currentProfit)>self.max_loss:
				logging.warning('交易亏损超过最大限度, 程序取消所有订单后退出!')
				self._cancelAllOrder()
				logging.waring('已停止！！！！')
				sys.exit()

			self.lastProfit=currentProfit
		return

	def getProfit(self):
		netNow=self.currentTotalBalance['balance']['total']+self.currentTotalBalance['stocks']['total']*self.averagePrice
		initNow=self.initTotalBalance['balance']['total']+self.initTotalBalance['stocks']['total']*self.averagePrice
		return self._floatFloor((netNow-initNow),8)

	def _cancelOrder(self):
		pass

	
	def _cancelAllOrder(self):
		since=None
		limit=None
		for p in self.exchanges:
			while True:
				orders=[]
				orders=p.exchange.fetchOpenOrders(p.symbol,since,limit)
				# orders=self.filter_orders_by_status(rawOrders, 'open')
				logging.debug('\n %s orders:%s',p.id,orders)
				time.sleep(self.interval/1000)
				if isinstance(orders,list):
					if len(orders)==0:
						break
					for order in orders:
						try:
							p.exchange.cancelOrder(order['id'],p.symbol)
							time.sleep(self.interval/1000)
						except Exception as e:
							logging.warning('errors during cancel orders %s:%s',order['id'],e)

	def onTick(self):
		if not self.isBalance:
			self.doBalance()
		else:
			self.doHedge()

		return



if __name__=='__main__':
	trade=hedge()
	trade.init_logger()
	trade.initExchanges(config)
	trade.initTotalBalance=trade.getAllBalance()
	logging.info('Total Balance: %s', trade.initTotalBalance)
	if trade.initTotalBalance['stocks']['total']==0 or trade.initTotalBalance['balance']['total']==0:
		logging.warning('所有交易所的币或钱总数为0，无法对冲')
		sys.exit()

	while trade.isNormal:
		tickStartTime=int(time.time()*1000)
		trade.onTick()
		tickEndTime=int(time.time()*1000)
		logging.info('tick lag time: %d ms',(tickEndTime-tickStartTime))
		time.sleep(trade.interval/1000)


