#設問2
#ネットワークの状態によっては、一時的にpingがタイムアウトしても、一定期間するとpingの応答が復活することがあり、
#そのような場合はサーバの故障とみなさないようにしたい。
#N回以上連続してタイムアウトした場合にのみ故障とみなすように、設問1のプログラムを拡張せよ。
#Nはプログラムのパラメータとして与えられるようにすること。

import pandas as pd

# csvファイルを読み込み, データフレーム化(日付はdatetime型に)
def csv_to_df(csv="ping_test/log/02.csv"):
    df = pd.read_csv(csv, names=["date", "address", "result"])
    df["date"] = pd.to_datetime(df["date"].astype(str))
    df = df.sort_values(["address", "date"])
    df = df.reset_index(drop=True)
    return df

# タイムアウトがn回以上含まれているアドレスを抽出, リスト化し返す
def timeouts(df, n):
    timeouts = df[df["result"] == "-"]["address"].tolist()
    timeouts = list(set(timeouts))
    for i in timeouts:
        to_count = ((df["address"] == i) & (df["result"] == "-")).sum()
        if to_count < n:
            timeouts.remove(i)
    return timeouts

def output(df, timeouts):
    errors_df = pd.DataFrame(columns=["address", "timeout_begin", "timeout_end", "consecutive", "duration"])
    for i in timeouts:
        con_count = 0
        latest_date = "0000-01-01 00:00:00"
        to_count = ((df["address"] == i) & (df["result"] == "-")).sum()
        for j in range(to_count):
            to_begin_x = df[(df["address"] == i) & (df["result"] == "-")].index[j]
            address = df.iloc[to_begin_x]["address"]
            begin_date = df.iloc[to_begin_x]["date"]
            to_end_x = df.iloc[to_begin_x:].loc[(df["address"] == i) & (df["result"] != "-")]
            if to_end_x.empty == True:
                begin_date = df.iloc[to_begin_x]["date"]
                end_date = ""
                duration = ""
            else:
                end_date = df.iloc[to_end_x.index[0]]["date"]
                if end_date == latest_date: #連続している場合の処理
                    con_count += 1
                latest_date = end_date
        if con_count >= n-1:
            oldest_date = df.iloc[to_begin_x - con_count]["date"] #タイムアウト開始日時を再設定(インデックスをずらす)
            duration = (latest_date - oldest_date).seconds
            data = pd.DataFrame({"address": [address], "timeout_begin": [oldest_date], 
                                "timeout_end": [latest_date], "consecutive":[con_count+1], "duration": [duration]})
            errors_df = pd.concat([errors_df, data])
    return errors_df

### 入力用 ###
n = int(input("何回連続タイムアウトしたものを故障とみなしますか(整数入力):"))
df = csv_to_df()
timeouts = timeouts(df, n)
output = output(df, timeouts)

print(df)
print(output)