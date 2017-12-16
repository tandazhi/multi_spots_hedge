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
import asyncio

# logging.basicConfig(level=logging.DEBUG,
# 	format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
# 	datefmt='%a, %d %b %Y %H:%M:%S',
# 	filename='myapp.log',
# 	filemode='w')

# 从config文件生产myExchange类， 包含ccxt类指针， apikey，交易费率， 初始balance等信息.
# 不支持okex期货，因为期货的账户权益与现货不同


class myExchange(object):
    # @retry

    def __init__(self, market):
        self.id = market['id']
        try:
            exec('self.exchange=ccxt.' + self.id + '()')
        except Exception as e:
            logging.error('%s init error, process stop!', market['id'])
            sys.exit()

        self.exchange.apiKey = market['api_key']
        self.exchange.secret = market['sec_token']
        self.exchange.loadMarkets()
        self.base, self.quote = re.split(r'/', market['symbol'])

        if market['symbol'] in self.exchange.symbols:
            self.symbol = market['symbol']
            self.market = self.exchange.markets[self.symbol]
        else:
            logging.warning('Exchange %s does not have the symbol %s, \
                            its stocks are set to 0, but its balance \
                            will be take into account!',
                            self.id, market['symbol'])
            self.market = None
            self.symbol = None

        self.feeTaker = market['feeTaker']
        self.feeMaker = market['feeMaker']

        self.depth = {}  # store the orderbook data
        self.balance = {}  # store the balance info of the account

    # @retry
    async def getBalance(self):
        balance = self.exchange.fetchBalance()

        if balance:
            if self.symbol is not None:
                self.balance = {'stocks': balance[
                    self.base], 'balance': balance[self.quote]}
            else:
                self.balance = {'stocks': {'free': 0, 'used': 0,
                    'total': 0}, 'balance': balance[self.quote]}
            return self.balance
        logging.error('%s getBalance return no data!!!', self.id)
        return

    async def getBalanceAsync(self):
        return await self.getBalance()
# ----------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------


class hedge(object):

    def __init__(self):
        self.exchanges = []
        self.initTotalBalance = {}
        self.currentTotalBalance = {}
        self.lastProfit = 0
        self.lastOpAmount = 0
        self.averagePrice = 0
        self.isBalance = True  # if the spot position is balance
        self.isNormal = True  # if the spot position is normal

    def init_logger(self):
        screenLevel = logging.INFO
        logLevel = logging.INFO
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S', level=screenLevel)
        Rthandler = RotatingFileHandler(
            'hedge.log', maxBytes=30 * 1024 * 1024, backupCount=30)
        Rthandler.setLevel(logLevel)
        formatter = logging.Formatter('%(asctime)s %(filename)s \
            [line:%(lineno)d] %(levelname)s %(message)s')
        Rthandler.setFormatter(formatter)
        logging.getLogger('').addHandler(Rthandler)
        return

    # to initialized the exchanges and running parameters
    def initExchanges(self, config):
        logging.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        self.exchanges = []

        if len(config.markets) < 2:
            logging.error(
                "Must have more than two markets to start the hedge!!!")
            sys.exit()
        for market in config.markets:
            try:
                self.exchanges.append(myExchange(market))
            except Exception as e:
                logging.error(e)

        self.interval = max(config.interval, 300)  # 出错重试间隔（毫秒）
        # 最低差价百分比（%），此对冲阈值只计算买卖盘差价，未考虑手续费。手续费一买一卖大约千分之四。
        self.minDiff = config.minDiff
        self.slideP = config.slideP  # 滑动价百分比(%)
        self.stopPL = config.stopPL  # 跌停值, 价格异常
        self.stopPH = config.stopPH  # 涨停值，价格异常
        self.minAmount = config.minAmount  # 单笔最小交易数量，
        self.maxAmount = config.maxAmount  # 单笔最大交易数量
        self.useMarketOrder = config.useMarketOrder  # 是否使用市价单止损?
        self.stop_when_loss = config.stop_when_loss  # 亏损时停止?
        self.max_loss = config.max_loss  # 最大亏损额
        self.maxLagTime = config.maxLagTime  # 各交易所间行情间隔最大时间, 秒
        self.slidingRatio = config.slidingRatio
        self.useExchangeMinAmount = config.useExchangeMinAmount

        minAmount = 0
        i = 0
        for p in self.exchanges:
            if p.symbol:
                minAmount = max(minAmount, p.market['limits'][
                                'amount']['min'])  # 交易所允许最小交易数量
                i += 1
        if i < 2:
            logging.error(
                "Less than two markets that have the symbol, can't hedge!!!")
            sys.exit()
        self.minAmount = minAmount if self.useExchangeMinAmount else max(
            self.minAmount, minAmount)  # 最小交易量
        if self.minAmount == 0:
            logging.error("min allowed amount to hedge is 0, process quit!!!")
            sys.exit()
        logging.info('程序设置参数: 轮询间隔%d毫秒, 最低差价百分比:%s, \
            单笔最小交易数量:%s,单笔最大交易数量:%s,最大允许亏损额:%s',
            self.interval, self.minDiff, self.minAmount, self.maxAmount,
            self.max_loss)

        return

    # 获取各交易所balance信息
    def getAllBalance(self):
        balanceNotOK = True
        tasks = []
        loop = asyncio.get_event_loop()
        while balanceNotOK:
            allBalance = {'stocks': {'free': 0, 'used': 0, 'total': 0},
                          'balance': {'free': 0, 'used': 0, 'total': 0}}

            try:
                for p in self.exchanges:
                    tasks.append(asyncio.ensure_future(p.getBalanceAsync()))
                loop.run_until_complete(asyncio.wait(tasks))

                for p in self.exchanges:
                    for key in p.balance.keys():
                        allBalance[key]['free'] += p.balance[key]['free']
                        allBalance[key]['used'] += p.balance[key]['used']
                        allBalance[key]['total'] += p.balance[key]['total']

                    logging.info('%s balance: %s', p.id.upper(), p.balance)
                balanceNotOK = False
            except Exception as e:
                logging.error(e)
        return allBalance

    # 获取各交易所交易对深度信息
    async def _getSingleDepth(self, p):
        p.depth = p.exchange.fetchOrderBook(p.symbol)
        return

    async def _getSingleDepthAsync(self, p):
        await self._getSingleDepth(p)

    def _getAllDepth(self):
        tasks = []
        loop = asyncio.get_event_loop()
        for p in self.exchanges:
            if p.symbol:
                # p.depth=p.exchange.fetchOrderBook(p.symbol)
                tasks.append(asyncio.ensure_future(
                    self._getSingleDepthAsync(p)))
        try:
            loop.run_until_complete(asyncio.wait(tasks))
        except Exception as e:
            logging.error("%s getOrderBook Error: %s", p.id, e)
        return

    # 执行此函数前， 需要先更新交易所账户信息getAllBalance，获取账户balance， 并且更新账户交易所深度信息getAllDepth，
    # 获取交易所orderbook
    def getMaxSpread(self):

        self._getAllDepth()
        maxBid, maxBidAmount, maxPair, minAsk, minAskAmount, minPair = [
            0, 0, None, 1000, 0, None]

        i = 0
        for p in self.exchanges:
            if p.depth:
                i += 1
        if i < 2:
            logging.error(
                "Less than two exchanges have order book data, can't hedge......")
            return maxBid, maxBidAmount, maxPair, minAsk, minAskAmount, minPair

        for p in self.exchanges:
            if p.symbol and p.depth:
                # 跳过盘口买一价, 且该交易所有足够币交易
                if maxBid < p.depth['bids'][1][0] and min(p.balance['stocks']['free'], p.depth['bids'][1][1]) >= self.minAmount:
                    maxBid = p.depth['bids'][1][0]
                    maxBidAmount = (p.depth['bids'][1][1] + p.depth['bids'][0][1])/3  #买一买二量和的三分之一
                    maxPair = p
                # 跳过盘口买一价, 且该交易所有足够钱交易
                if minAsk > p.depth['asks'][1][0] and p.depth['asks'][1][1] >= self.minAmount and self._floatFloor(p.balance['balance']['free'], 6) > p.depth['asks'][1][0] * self.minAmount * (1 + p.feeTaker + self.slideP / 100):
                    minAsk = p.depth['asks'][1][0]
                    minAskAmount = (p.depth['asks'][1][1] + p.depth['asks'][0][1])/3  #买一买二量和的三分之一
                    minPair = p
        if minPair and maxPair:
            logging.info("max bid exchange:%s, max bid price:%s, max bid amount:%s; min ask exchange:%s, min ask price:%s, min ask amount:%s, spread percent:%f",
                         maxPair.id, maxBid, maxBidAmount, minPair.id, minAsk, minAskAmount, (maxBid / minAsk - 1) * 100)
            self.averagePrice = (maxPair.depth['bids'][0][
                                 0] + minPair.depth['bids'][0][0]) / 2
        return maxBid, maxBidAmount, maxPair, minAsk, minAskAmount, minPair

    def doHedge(self):

        maxBid, maxBidAmount, maxPair, minAsk, minAskAmount, minPair = self.getMaxSpread()

        if (maxPair == None) or (minPair == None):  # 未筛选出对冲交易所， 返回
            return

        # 未筛选出对冲交易所， 返回
        if maxBidAmount == 0 or minAskAmount == 0 or (maxPair.id == minPair.id) or (maxBid < minAsk):
            return

        if (not self._isPriceNormal(maxBid)) or (not self._isPriceNormal(minAsk)):  # 价格不正常， 返回
            logging.warning('The price is unormal, jump over this cycle......')
            return

        if abs(maxPair.depth['timestamp'] - minPair.depth['timestamp']) > self.maxLagTime * 1000 or abs(time.time() * 1000 - maxPair.depth['timestamp']) > self.maxLagTime * 1000:  # 深度信息时间差大于阈值则不操作
            logging.warning(
                'the time lag between different exchanges is bigger than 3 seconds！！！！！')
            return

        # orderbook 价格不正常， 返回
        if maxPair.depth['asks'][0][0] < maxPair.depth['bids'][0][0] or maxPair.depth['asks'][0][0] > maxPair.depth['asks'][1][0] \
                or minPair.depth['asks'][0][0] < minPair.depth['bids'][0][0] or minPair.depth['asks'][0][0] > minPair.depth['asks'][1][0]:
            logging.warning(
                'Depth info is unormal, jump over this cycle......')
            return

        if maxBid >= minAsk * (1 + self.minDiff / 100):  # 价差超过对冲阈值，则执行如下对冲操作
            logging.debug('%s balance: %s. %s balance: %s', maxPair.id,
                          maxPair.balance, minPair.id, minPair.balance)
            canSellAmount = min(maxPair.balance['stocks'][
                                'free'], maxBidAmount, self.maxAmount)
            canBuyAmount = minPair.balance['balance'][
                'free'] / minAsk / (1 + self.slideP / 100 + minPair.feeTaker)
            # hedgeAmount=self._floatFloor(min(canSellAmount,canBuyAmount),2)
            # #对冲币数量取2位小数
            hedgeAmount = self.adjustAmountFloor(
                min(canSellAmount, canBuyAmount))  # 对冲币数量取交易所允许最小数量的整数倍
            logging.info('%s can sell %f, %s can buy %f.',
                         maxPair.id, canSellAmount, minPair.id, canBuyAmount)
            self.lastOpAmount = hedgeAmount  # 记录每次的操作币数量
            logging.info('Exchange %s buy %f coins; Exchange %s sell %f coins.',
                         minPair.id, hedgeAmount, maxPair.id, hedgeAmount)
            try:
                sellOrder = maxPair.exchange.createLimitSellOrder(
                    maxPair.symbol, hedgeAmount, self._floatFloor(maxBid * (1 - self.slideP / 100), 8))
                if(sellOrder):
                    buyOrder = minPair.exchange.createLimitBuyOrder(
                        minPair.symbol, hedgeAmount, self._floatCeil(minAsk * (1 + self.slideP / 100), 8))
                logging.info('%s sell order: %s and %s buy order: %s placed',
                             maxPair.id, sellOrder['id'], minPair.id, buyOrder['id'])
            except Exception as e:
                logging.error(
                    "Error during placing hedge orders: Error: %s", e)
            self.isBalance = False

        return

    def filter_orders_by_status(self, orders, status):
        result = []
        for i in range(0, len(orders)):
            if orders[i]['status'] == status:
                result.append(orders[i])
        return result

    def _isPriceNormal(self, price):
        if price > self.stopPH or price < self.stopPL:
            return False
        return True

    def _floatFloor(self, number, decimal=8):
        return math.floor(number * 10**decimal) / 10**decimal

    def _floatCeil(self, number, decimal=8):
        return math.ceil(number * 10**decimal) / 10**decimal

    def doBalance(self):
        self._cancelAllOrder()
        self.currentTotalBalance = self.getAllBalance()
        # stockDiff=self._floatFloor((self.currentTotalBalance['stocks']['total']-self.initTotalBalance['stocks']['total']),2)
        stockDiff = self._floatFloor(self.currentTotalBalance['stocks'][
                                     'total'] - self.initTotalBalance['stocks']['total'], 3)
        if abs(stockDiff) > 1.11 * self.maxAmount:
            self.isNormal = False
            logging.warning("仓位变动异常!仓位差：%f, 停止交易", stockDiff)
            sys.exit()

        if abs(stockDiff) < self.minAmount:
            self.isBalance = True

        else:
            orderAmount = 0
            logging.info('初始币总数量:%s; 现在币总数量:%s; 差额:%s', self.initTotalBalance['stocks'][
                         'total'], self.currentTotalBalance['stocks']['total'], stockDiff)

            maxBid, maxBidAmount, maxPair, minAsk, minAskAmount, minPair = self.getMaxSpread()

            if (maxPair == None) or (minPair == None) or maxBidAmount == 0 or minAskAmount == 0:  # 未筛选出对冲交易所， 返回
                return

            if (not self._isPriceNormal(maxBid)) or (not self._isPriceNormal(minAsk)):  # 价格不正常， 返回
                logging.warning(
                    'The price is unormal, jump over this cycle......')
                return

            if abs(maxPair.depth['timestamp'] - minPair.depth['timestamp']) > self.maxLagTime * 1000 or abs(time.time() * 1000 - maxPair.depth['timestamp']) > self.maxLagTime * 1000:  # 深度信息时间差大于阈值则不操作
                logging.warning(
                    'the time lag between different exchanges is bigger than 3 seconds！！！！！')
                return

            # orderbook 价格不正常， 返回
            if maxPair.depth['asks'][0][0] < maxPair.depth['bids'][0][0] or maxPair.depth['asks'][0][0] > maxPair.depth['asks'][1][0] \
                    or minPair.depth['asks'][0][0] < minPair.depth['bids'][0][0] or minPair.depth['asks'][0][0] > minPair.depth['asks'][1][0]:
                logging.warning(
                    'Order Book Information is unormal, jump over this cycle......')
                return

            # self.lastOpAmount=stockDiff

            if stockDiff > 0:
                orderAmount = self.adjustAmountFloor(
                    min(stockDiff, maxPair.balance['stocks']['free']))
                if self.useMarketOrder:
                    orderPrice = self._floatFloor(
                        maxBid * (1 - self.slideP / 100), 8)
                    logging.info('仓位平衡中, 交易所%s市价卖出币数量%s, 挂单价：%s',
                                 maxPair.id, orderAmount, orderPrice)
                else:
                    orderPrice = self._floatFloor(maxPair.depth['asks'][0][
                                                  0] - (maxPair.depth['asks'][0][0] - maxPair.depth['bids'][0][0]) / self.slidingRatio, 8)
                    logging.info('仓位平衡中, 交易所%s限价卖出币数量%s, 挂单价：%s',
                                 maxPair.id, orderAmount, orderPrice)
                try:
                    order = maxPair.exchange.createLimitSellOrder(
                        maxPair.symbol, orderAmount, orderPrice)
                    logging.info('Order %s placed in %s to balance position: %s', order[
                                 'id'], maxPair.id, order['info'])
                except Exception as e:
                    logging.error("%s placing order Error: %s", maxPair.id, e)

            else:
                stockDiff = abs(stockDiff)
                if self.useMarketOrder:
                    orderPrice = self._floatCeil(
                        minAsk * (1 + self.slideP / 100), 8)
                    canBuyAmount = minPair.balance['balance'][
                        'free'] / orderPrice  # 交易所余钱能买多少币
                    orderAmount = self.adjustAmountRound(stockDiff) if (
                        canBuyAmount - stockDiff) > self.minAmount else self.adjustAmountFloor(min(stockDiff, canBuyAmount))
                    logging.info('仓位平衡中, 交易所%s市价买入币数量%s, 挂单价：%s',
                                 minPair.id, orderAmount, orderPrice)

                else:
                    orderPrice = self._floatFloor(minPair.depth['bids'][0][
                                                  0] + (minPair.depth['asks'][0][0] - minPair.depth['bids'][0][0]) / self.slidingRatio, 8)
                    canBuyAmount = minPair.balance['balance'][
                        'free'] / orderPrice  # 交易所余钱能买多少币
                    orderAmount = self.adjustAmountRound(stockDiff) if (
                        canBuyAmount - stockDiff) > self.minAmount else self.adjustAmountFloor(min(stockDiff, canBuyAmount))
                    orderPrice = self._floatFloor(maxPair.depth['asks'][0][
                                                  0] - (maxPair.depth['asks'][0][0] - maxPair.depth['bids'][0][0]) / self.slidingRatio, 8)
                    logging.info('仓位平衡中, 交易所%s限价买入币数量%s, 挂单价：%s',
                                 minPair.id, orderAmount, orderPrice)

                try:
                    order = minPair.exchange.createLimitBuyOrder(
                        minPair.symbol, orderAmount, orderPrice)
                    logging.info('Order %s placed in %s to balance position: %s', order[
                                 'id'], minPair.id, order['info'])
                except Exception as e:
                    logging.error("%s placing order Error: %s", minPair.id, e)

        if self.isBalance and self.lastOpAmount:
            currentProfit = self.getProfit()
            logging.info('Total Profit: %s; This time profit:%s, Spread: %s; Balance: %s, Stocks: %s.', currentProfit, (currentProfit - self.lastProfit),
                         (currentProfit - self.lastProfit) / self.lastOpAmount, self.currentTotalBalance['balance']['total'], self.currentTotalBalance['stocks']['total'])
            if self.stop_when_loss and currentProfit < 0 and abs(currentProfit) > self.max_loss:
                logging.warning('交易亏损超过最大限度, 程序取消所有订单后退出!')
                self._cancelAllOrder()
                logging.waring('已停止！！！！')
                sys.exit()

            self.lastProfit = currentProfit
        return

    def getProfit(self):
        netNow = self.currentTotalBalance['balance'][
            'total'] + self.currentTotalBalance['stocks']['total'] * self.averagePrice
        initNow = self.initTotalBalance['balance'][
            'total'] + self.initTotalBalance['stocks']['total'] * self.averagePrice
        return self._floatFloor((netNow - initNow), 8)

    def _cancelOrder(self):
        pass

    def adjustAmountFloor(self, amount):
        return math.floor(amount / self.minAmount) * self.minAmount if self.minAmount != 0 else 0

    def adjustAmountRound(self, amount):
        return round(amount / self.minAmount) * self.minAmount if self.minAmount != 0 else 0

    async def __cancelOrders(self, p):  # p is myExchange class
        since = None
        limit = None
        if p.symbol:
            while True:
                orders = []
                try:
                    orders = p.exchange.fetchOpenOrders(
                        p.symbol, since, limit)
                except Exception as e:
                    break

                logging.debug('\n %s orders:%s', p.id, orders)
                # time.sleep(self.interval / 1000)
                if isinstance(orders, list):
                    if len(orders) == 0:
                        break
                    for order in orders:
                        try:
                            p.exchange.cancelOrder(order['id'], p.symbol)
                            logging.info('%s cancel order %s: %s', p.id, order[
                                         'id'], order['info'])
                            # time.sleep(self.interval / 1000)
                        except Exception as e:
                            logging.error('errors during cancel orders %s:%s',
                                                        order['id'], e)

    async def __cancelOrdersAsync(self, p):
        await self.__cancelOrders(p)

    def _cancelAllOrder(self):
        tasks = []
        loop = asyncio.get_event_loop()
        for p in self.exchanges:
            tasks.append(asyncio.ensure_future(self.__cancelOrdersAsync(p)))
        loop.run_until_complete(asyncio.wait(tasks))

    def onTick(self):
        if not self.isBalance:
            self.doBalance()
        else:
            self.doHedge()

        return


if __name__ == '__main__':
    trade = hedge()
    trade.init_logger()
    trade.initExchanges(config)
    trade.initTotalBalance = trade.getAllBalance()

    logging.info('Total Balance: %s', trade.initTotalBalance)
    if (trade.initTotalBalance['stocks']['total'] == 0 or
            trade.initTotalBalance['balance']['total'] == 0):
        logging.warning('所有交易所的币或钱总数为0，无法对冲')
        sys.exit()

    while trade.isNormal:
        tickStartTime = int(time.time() * 1000)
        trade.onTick()
        tickEndTime = int(time.time() * 1000)
        logging.info('tick lag time: %d ms', (tickEndTime - tickStartTime))
        time.sleep(trade.interval / 1000)

    # p=trade.exchanges[1]
    # print(p.exchange.markets['EOS/BTC'])
    # since=None
    # limit=None
    # order=p.exchange.createLimitSellOrder('EOS/BTC',3,0.1)
    # print(order)
    # time.sleep(1)
    # p.exchange.cancelOrder(order['id'],'EOS/BTC')
