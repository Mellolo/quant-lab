import datetime
import pandas as pd
from backtest.feeds.clean import data_clean_with_merge
from backtest.manual.backtest import ManualBacktestEngine, ManualSignal

def backtest():
    engine = ManualBacktestEngine()
    data_df = pd.read_csv('test/600150.XSHG.csv')
    data_df = data_clean_with_merge(data_df,
                                    datetime.date(2025, 7, 1),
                                    datetime.date(2025, 9, 15),
                                    [("09:30", "11:30"), ("13:00", "15:00")],
                                    "5m", "30m")
    engine.add_data(data_df)
    engine.set_from_datetime(datetime.datetime(2025, 8, 1))
    engine.run()

    first = True
    while True:
        info = engine.get_info()
        df = info.get_arg("data")
        if info.get_arg("stop"):
            break
        if first:
            engine.send_signal(ManualSignal(ManualSignal.Open))
            first = False
        else:
            engine.send_signal(ManualSignal(ManualSignal.Continue))
    engine.plot()

if __name__ == '__main__':
    backtest()
    # from http_service.app import app
    #
    # print("启动HTTP服务...")
    # app.run(host='0.0.0.0', port=9990, debug=True)