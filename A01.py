# 設問1
#監視ログファイルを読み込み、故障状態のサーバアドレスとそのサーバの故障期間を出力するプログラムを作成せよ。
#出力フォーマットは任意でよい。
#なお、pingがタイムアウトした場合を故障とみなし、最初にタイムアウトしたときから、次にpingの応答が返るまでを故障期間とする。

import numpy as np
import pandas as pd

logs = pd.read_csv("ping_test/log/01.csv", names=["date", "address", "result"])
logs["date"] = pd.to_datetime(logs["date"].astype(str))


# タイムアウトが含まれているアドレスを抽出
timeouts = logs[logs["result"] == "-"]["address"].tolist()
timeouts = list(set(timeouts))
#print(logs)
print(timeouts)

print(logs[logs["address"].isin(timeouts)])

error_time = 0


    
    
    



