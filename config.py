# Markets and Access to Private APIs
#对轮询方式来说， 先接收Lag time长的交易所行情， 可以减小多个交易所之间行情的时间差。因此以下设置中， 将耗时长的交易所放在前面.
markets = [
# {id:'binance',symbol:'LTC/BTC',api_key:'',sec_token:'',fee:0.0025},
#{'id':'bitfinex','symbol':'LTC/BTC','api_key':'','sec_token':'','fee':0.0025},
# {id:'bitstamp',symbol:'LTC/BTC',api_key:'',sec_token:'',fee:0.0025},
# {id:'bithumb',symbol:'LTC/BTC',api_key:'',sec_token:'',fee:0.0025},
# {id:'bittrex',symbol:'LTC/BTC',api_key:'',sec_token:'',fee:0.0025},
# {id:'bitstamp',symbol:'LTC/BTC',api_key:'',sec_token:'',fee:0.0025},
# {id:'Kraken',symbol:'LTC/BTC',api_key:'',sec_token:'',fee:0.0025},
{'id':'poloniex','symbol':'LTC/BTC','api_key':'','sec_token':'','feeTaker':0.0025,'feeMaker':0.0025},
#{'id':'huobipro','symbol':'LTC/BTC','api_key':'','sec_token':'','feeTaker':0.002,'feeMaker':0.002},
{'id':'okex','symbol':'LTC/BTC','api_key':'','sec_token':'','feeTaker':0.001,'feeMaker':0.001},
#{'id':'okex','symbol':'BTC/USD','api_key':'','sec_token':'','feeTaker':0.0025,'feeMaker':0.0025},
#{'id':'okex','symbol':'LTC/BTC','api_key':'','sec_token':'','feeTaker':0.0025,'feeMaker':0.0025},
#{'id':'bitmex','symbol':'BTC/USD','api_key':'','sec_token':'','feeTaker':0.0025,'feeMaker':0.0025}
]

#运行参数
interval=350 #检测间隔（毫秒）
minDiff=0.6 #最低差价百分比（%）
slideP=0.2 #止损滑动价百分比(%)
stopPL=0.001 #跌停值, 价格异常
stopPH=0.1 #涨停值，价格异常
minAmount=0.2 #单笔最小交易数量
maxAmount=1 #单笔最大交易数量
useMarketOrder=False #是否使用市价单止损?
stop_when_loss=False #亏损时停止?
max_loss=0.01 #最大亏损额
maxLagTime=3 #各交易所允许行情间隔最大时间, 秒
# 



