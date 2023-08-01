# 設問1
#監視ログファイルを読み込み、故障状態のサーバアドレスとそのサーバの故障期間を出力するプログラムを作成せよ。
#出力フォーマットは任意でよい。
#なお、pingがタイムアウトした場合を故障とみなし、最初にタイムアウトしたときから、次にpingの応答が返るまでを故障期間とする。

import pandas as pd

# csvファイルを読み込み, データフレーム化(日付はdatetime型に)
logs = pd.read_csv("ping_test/log/01_06.csv", names=["date", "address", "result"])
logs["date"] = pd.to_datetime(logs["date"].astype(str))

# タイムアウトが含まれているアドレスを抽出しリスト化
timeouts = logs[logs["result"] == "-"]["address"].tolist()
timeouts = list(set(timeouts))

# リストに含まれているアドレスのみのデータフレーム作成(+ソート)
logs_to = logs[logs["address"].isin(timeouts)]
logs_to = logs_to.sort_values(["address", "date"])
logs_to = logs_to.reset_index(drop=True)

# 出力用のデータフレーム
errors_df = pd.DataFrame(columns=["address", "timeout_begin", "timeout_end", "duration"])

for i in timeouts:
    to_count = ((logs_to["address"] == i) & (logs_to["result"] == "-")).sum()
    for j in range(to_count):
        to_begin_x = logs_to[(logs_to["address"] == i) & (logs_to["result"] == "-")].index[j]
        address = logs_to.iloc[to_begin_x]["address"]
        begin_date = logs_to.iloc[to_begin_x]["date"]
        to_end_x = logs_to.iloc[to_begin_x:].loc[(logs_to["address"] == i) & (logs_to["result"] != "-")]
        if to_end_x.empty == True:
            end_date = ""
            duration = ""
        else:
            end_date = logs_to.iloc[to_end_x.index[0]]["date"]
            duration = (end_date - begin_date).seconds
        data = pd.DataFrame({"address": [address], "timeout_begin": [begin_date], 
                            "timeout_end": [end_date], "duration": [duration]})
        errors_df = pd.concat([errors_df, data])

# 出力
errors_df = errors_df.sort_values(["address", "timeout_begin"])
errors_df = errors_df.reset_index(drop=True)
print(errors_df)
