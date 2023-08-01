#設問2
#ネットワークの状態によっては、一時的にpingがタイムアウトしても、一定期間するとpingの応答が復活することがあり、
#そのような場合はサーバの故障とみなさないようにしたい。
#N回以上連続してタイムアウトした場合にのみ故障とみなすように、設問1のプログラムを拡張せよ。
#Nはプログラムのパラメータとして与えられるようにすること。

import numpy as np
import pandas as pd

class Test_02:

    def __init__(self, n):
        self.n = n
        self.output_df = pd.DataFrame(columns=["address", "timeout_begin", "timeout_end", "consecutive", "duration"])
        
    # csvファイルを読み込み, データフレーム化(日付はdatetime型に)
    def csv_to_df(self, csv="ping_test/log/02.csv"):
        df = pd.read_csv(csv, names=["date", "address", "result"])
        df["date"] = pd.to_datetime(df["date"].astype(str))
        df = df.sort_values(["address", "date"])
        df = df.reset_index(drop=True)
        self.df = df
    
    # タイムアウト連続回数チェック
    def consecutive_check(self):
        consecutive_df = pd.DataFrame(columns=["address", "index_begin", "index_end",])
        sorted_df = self.df[self.df["result"] == "-"]
        sorted_df = sorted_df.sort_values(["result", "address", "date"])
        index_list = list(sorted_df.index)
        index2 = []
        numbers = [] #連続しているインデックス番号のリスト
        addresses = [] #そのときのアドレスのリスト
        
        for x, y in sorted_df.groupby("address"):
            for z in y.index:
                index2.append(z)

        #print(sorted_df)
        for j in index2:
            if not numbers or j > numbers[-1][-1] +1:
                numbers.append([j])
            else:
                numbers[-1].append(j)
        numbers = [x for x in numbers if len(x) >= self.n] #指定したn回以上連続しているものだけ抽出
        for k in range(len(numbers)):
            addresses.append(self.df.iloc[numbers[k][0]]["address"])
        self.numbers = numbers
        self.addresses = addresses
    
    def output(self):
        output_df = pd.DataFrame(columns=["address", "timeout_begin", "timeout_end", "consecutive", "duration"])
        for m in range(len(self.numbers)):
            #タイムアウトの後復帰しているか確認
            #print(self.numbers[m][-1], "以降をチェック")
            con_n = len(self.numbers[m]) #連続回数
            address = self.addresses[m]
            begin = self.df.iloc[self.numbers[m][0]]["date"]
            y = self.numbers[m][-1]
            x = self.df.iloc[y:].loc[(self.df["address"] == self.addresses[m]) & (self.df["result"] != "-")]
            if x.empty == True:
                end = np.nan
                duration = np.nan
            else: #復帰している場合
                end = self.df.iloc[x.index[0]]["date"]
                duration = (end - begin).seconds
            data = pd.DataFrame({"address": [address], "timeout_begin": [begin], "consecutive": [con_n],
                                "timeout_end": [end], "duration": [duration]})
            output_df = pd.concat([output_df, data])
        output_df = output_df.reset_index(drop=True)
        return output_df

### 出力用 ###
n = int(input("何回連続タイムアウトしたものを故障とみなしますか(整数入力):"))
test = Test_02(n)
test.csv_to_df()
test.consecutive_check()
print(test.output())


#df = csv_to_df()
#timeouts = timeouts(df, n)
#output = output(df, timeouts)
#index_list = consecutive_check(df, timeouts)

#print(df)
#print(timeouts)
#print(output)