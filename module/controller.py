import time
import pytz

from datetime import timezone as tz
from mDataStore.bloomberg import blpEventHandler, blp, Dict, dt, pd, tqdm
from mDataStore.globalMongo import mds
from threading import Lock, Thread
from warnings import warn


class ShooterOnline(blpEventHandler):

    def __init__(self):

        # call parent constructor
        super().__init__()

        # Bloomberg connection object, setting this object it self as a handler
        print("Establishing BLP connection.")
        self.blp_i = blp(eventHandler=self)

        # bloomberg tickers list to subscribe and a map to the assets [bloomberg -> assets]
        self.tickers = None
        self.assets = None
        self.asset_tickers = None
        self.meta = {}
        self.risk_mid = {}

        # set memory
        self.memory = {}

        # create a mutex for update synchronization
        self.mutex = Lock()

        self.today = dt.now()

    @staticmethod
    def polling(handler):

        while True:
            # prevents cpu 100% and establish a data update interval into mongodb [snapshot] collection
            time.sleep(15)

            # save memory data into mongodb each 60 min
            handler.update()

    def onBlpReceive(self, data, topic, dti):

        # print("Event received.")
        with self.mutex:

            if topic not in self.memory:
                self.memory[topic] = {}

            if "LAST_PRICE" in data:
                self.memory[topic]["LAST_PRICE"] = data["LAST_PRICE"]

            if "VOLUME" in data:
                self.memory[topic]["VOLUME"] = data["VOLUME"]

            if "TIME" in data:
                try:
                    self.memory[topic]["TRADE_TIME"] = dt.combine(self.today, data["TIME"])
                except TypeError:
                    self.memory[topic]["TRADE_TIME"] = dt.combine(data["TIME"], dt.time(dt(1970, 1, 1, 0, 0, 0)))

            if "TRADE_UPDATE_STAMP_RT" in data:
                self.memory[topic]["TRADE_TIME"] = data["TRADE_UPDATE_STAMP_RT"]

            # print("Memory: {}".format(self.memory))

    def start(self):

        # assure that bbg communication object was instanced.
        assert self.blp_i

        # clear output collection in mongodb
        mds.mongoCli.db_dws.mgmt.snapshot.drop()

        # load assets from metadata worksheet.
        self.load_asset_tickers()
        print("Asset tickers loaded: ", str(len(self.assets)))

        # get intraday data available until now and insert them into mongodb (1 minute bar)
        self.load_asset_data(assets=[
            "DI1F22", "DI1F23", "DI1F25", "DI1F27",
            "TY1", "TU1", "ES1", "EC1",
            "FUT_GOLD_2", "FUT_HEATING_OIL_2", "FUT_SILVER_2", "FUT_SP_COMMODITY", "FUT_WTI_2", "FUT_ZAR_USD",
            "FUT_AUD_USD", "FUT_CAD_USD", "FUT_CHF_USD", "FUT_GBP_USD", "FUT_JPY_USD", "FUT_NZD_USD",
            "PETR4", "BBDC4",
            "UC1", "BZ1",
            "WDOG21", "WING21"
        ])

        print("Subscribing...")
        print(self.tickers)
        self.blp_i.subscribe(self.tickers, ['LAST_PRICE', 'BID', 'ASK', 'VOLUME', 'TIME'], ['interval=15'])

        # update a first time
        self.update()

        # calculate day CDI
        self.calculate_cdi()

        # update cash_usd/cash_px fisk free reference.
        self.caculate_riskfree_us()

        # load risk_mid
        self.load_risk_mid()

        # create an index to decrease consulting time
        idx_creation = mds.mongoCli.db_dws.mgmt.snapshot.create_index([("trade_time", -1)])
        print(idx_creation)

        # halt
        print("Start polling thread.")
        t = Thread(target=ShooterOnline.polling, args=(self,))
        t.start()
        pass

    def load_asset_tickers(self):
        """
        Load assets metadata from momgodb
        :return: void
        """
        # get metadata of used assets from mongodb [db_dws.mgmt.metadata].
        #
        # Debug purpose - Load fewer assets.
        # assets_meta = list(mds.mongoCli.db_dws.mgmt.metadata.find({"in_use": 1, "feeder_id": {"$exists": True},
        # "field_hist": "equity"}))
        assets_meta = list(mds.mongoCli.db_dws.mgmt.metadata.find({"in_use": 1, "feeder_id": {"$exists": True}}))

        # read query result and populate assets dictionary (map)
        self.assets = Dict({})
        self.asset_tickers = Dict({})

        for meta in assets_meta:

            # save all meta info.
            self.meta[meta['feeder_id']] = meta

            if meta['feeder_id'] not in self.assets:
                self.assets[meta['feeder_id']] = [meta['name']]
            else:
                if meta['name'] not in self.assets[meta['feeder_id']]:
                    self.assets[meta['feeder_id']].append(meta['name'])

            self.asset_tickers[meta['name']] = meta['feeder_id']

        # check if there at least one asset.
        if len(self.assets) > 0:
            self.tickers = list(self.assets.keys())

    def update(self):
        """
        Update ticker prices requesting Bloomberg api
        :return: void - self contained method.
        """
        # get current time
        now = dt.now()
        print("Update procedure executed at [", now, '].')

        to_insert = []

        with self.mutex:

            for topic in self.memory:

                if topic in self.memory and 'LAST_PRICE' in self.memory[topic] and 'VOLUME' in self.memory[topic] and \
                        'TRADE_TIME' in self.memory[topic]:

                    trade_time = self.memory[topic]["TRADE_TIME"]
                    trade_time = pd.Timestamp(trade_time).tz_localize(None)

                    for asset in self.assets[topic]:
                        to_insert.append({
                            'timestamp': now,
                            'asset': asset,
                            'last_px': self.memory[topic]['LAST_PRICE'],
                            'volume': self.memory[topic]['VOLUME'],
                            'trade_time': trade_time
                        })
                else:
                    print("Data not complete or not found for ticker: " + topic)

            if not to_insert:
                print("No data in memory to insert into database.")
            else:
                mds.mongoCli.db_dws.mgmt.snapshot.insert_many(to_insert)
                print(to_insert)

        print("Memory: {}".format(self.memory))

        print("Data successfully updated [", str(len(to_insert)), "]")

    def get_data(self, assets):
        """
        Return data for a list of assets
        :param assets:
        :return:
        """
        ret = {}
        for a in assets:
            if a in self.memory:
                ret[a] = self.memory[a]

        return ret

    def get_risk_mid(self, ticker):
        """
        Return bbg risk mid data of yield ticker.
        :param ticker: about to be searched.
        :return: risk mid data of [ticker] - None if it is not in control.
        """
        return self.risk_mid[ticker] if ticker in self.risk_mid else None

    def get_multiplier(self, ticker):
        """
        Return a multiplier of bbg [ticker]
        :param ticker: Ticker about to be searched.
        :return: Mutiplier value of the ticker.
        """
        return (self.meta[ticker]['multiplier'] if 'multiplier' in self.meta[ticker] else None) \
            if ticker in self.meta else None

    def load_risk_mid(self):

        tickers = []

        for k in self.meta.keys():

            meta = self.meta[k]

            if "type" in meta and meta["type"] and meta["type"] == "future" and meta["subtype"] == "di_fut":
                tickers.append(k)

        if not tickers:
            warn("No tickers found to load [RISK_MID]")
            return

        dd = self.blp_i.getRefData(tickers, ['RISK_MID']).to_dict()

        for asset in dd["RISK_MID"].keys():
            self.risk_mid[asset] = dd["RISK_MID"][asset]

    @staticmethod
    def calculate_cdi():

        # get current timestamp
        now = pd.Timestamp(dt.now())
        now = now.normalize()

        # retrieve current tax
        df_tx = mds.read('CDI_1DAY')

        # retrieve current factor
        df_fact = mds.read('CDI')

        # calculate current CDI
        cdi1 = df_fact.close[-1] * (1 + df_tx.close[-1] / 100) ** (1 / 252)

        print("Calculating CDI.")

        # insert calculated value into [shooter] database
        mds.mongoCli.db_dws.mgmt.snapshot.insert_one({
            'timestamp': now.to_pydatetime(),
            'asset': 'CDI',
            'last_px': cdi1,
            'trade_time': now
        })

    @staticmethod
    def caculate_riskfree_us():

        # get current timestamp
        now = pd.Timestamp(dt.now())
        now = now.normalize()

        # retrieve current tax
        df_cash_cx = mds.read('CASH_CX')

        print("Inserting CASH CX for the day.")

        # insert calculated value into [shooter] database
        mds.mongoCli.db_dws.mgmt.snapshot.insert_one({
            'timestamp': now.to_pydatetime(),
            'asset': 'CASH_CX',
            'last_px': df_cash_cx.iloc[-1]['close'],
            'trade_time': now
        })

        # retrieve current tax
        df_cash_usd = mds.read('CASH_USD')

        print("Inserting CASH USD for the day. Proceeding...")

        # insert calculated value into [shooter] database
        mds.mongoCli.db_dws.mgmt.snapshot.insert_one({
            'timestamp': now.to_pydatetime(),
            'asset': 'CASH_USD',
            'last_px': df_cash_usd.iloc[-1]['close'],
            'trade_time': now
        })

    def load_asset_data(self, assets=None):

        print("Loading intraday data")

        # get start and end time for the request interval.
        start = dt.now()
        start = start.replace(hour=9, minute=0, second=0, microsecond=0)
        start = start.astimezone(tz.utc)

        end = dt.now()
        end = end.astimezone(tz.utc)

        tickers = []

        if not assets:
            #
            # ATTENTION! It should not be used, It might take too much time to process and stress the Bloomberg API
            # limits.
            #
            # if no filter was defined, it will use all tickers.
            tickers = self.tickers

        else:
            for asset in assets:
                tickers.append(self.asset_tickers[asset])

        if not tickers:
            raise Exception("No tickers set to request intraday data.")

        asset_data = self.blp_i.getIntradayHistoricData(tickers, "1", start, end)

        to_insert = []
        c = 0
        for ticker in tqdm(tickers):
            # get current data related to the ticker
            ad = asset_data[c]

            # bring timestamp to [Brazil/São Paulo] timezone
            ad.index = ad.index.tz_localize(pytz.utc).tz_convert(pytz.timezone("America/Sao_Paulo")).tz_localize(None)

            # drop unnecessary columns
            ad = ad.drop(["open", "high", "low", "numEvents", "value"], axis=1)

            # insert index into dataframe
            ad.reset_index(inplace=True)

            # append trade_time column
            ad['trade_time'] = ad['time']

            # rearrange columns
            ad = ad[['time', 'asset', 'close', 'volume', 'trade_time']]

            # rename columns
            ad = ad.rename(columns={'time': 'timestamp', 'close': 'last_px'})

            # append asset column
            for t1 in self.assets[ticker]:
                ad['asset'] = t1

                to_insert.extend(ad.to_dict("r"))

            # set counter for the next step
            c += 1

        # insert data into [snapshot] collection in mongodb.
        mds.mongoCli.db_dws.mgmt.snapshot.insert_many(to_insert)
        pass
