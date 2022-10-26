
from nfstream import NFStreamer
import pandas as pd
from sklearn.ensemble import RandomForestClassifier



# Globals
timestamp = 'bidirectional_first_seen_ms'
src_ip = 'src_ip'
dst_ip = 'dst_ip'
dst_host = 'requested_server_name'
dst_port = 'dst_port'
bytes_sent = 'src2dst_bytes'




def Beacon_analysis(file):




   filter = [timestamp, src_ip, dst_ip, dst_host, dst_port, bytes_sent]
   groupby = [src_ip, dst_ip, dst_port] #Group the connect together that are the same. 

   df = df.loc[:,filter]
   df[timestamp] = pd.to_datetime(df[timestamp], unit='ms')  #Converting ms to datetime
   df = df.groupby(groupby).agg(list)
   df.reset_index(inplace=True)


   #ConnectionCount is  by taking each row in the timestamp column, and get the about of connection that have been made
   df['ConnectionCount'] = df[timestamp].apply(lambda x: len(x))

   #Remove all connection with less then 10 connections, it was choosen because of the small data sample I used, The goal is to reduce the amount of data that need to be processed
   df = df.loc[df['ConnectionCount'] > 10]

   #Sort the data
   df[timestamp] = df[timestamp].apply(lambda x: sorted(x))


   df['delta_time'] = df[timestamp].apply(lambda x: pd.Series(x).diff().dt.seconds.dropna().tolist())


   df['tsLow'] = df['delta_time'].apply(lambda x: np.percentile(np.array(x),25))
   df['tsMid'] = df['delta_time'].apply(lambda x: np.percentile(np.array(x), 50))
   df['tsHigh'] = df['delta_time'].apply(lambda x: np.percentile(np.array(x), 75))

   df['tsBowleyNum'] = df['tsLow'] + df['tsHigh'] - 2 * df['tsMid']
   df['tsBowleyDen'] = df['tsHigh'] - df['tsLow']

   # tsSkew should equal zero if the denominator equals zero
   # bowley skew is unreliable if Q2 = Q1 or Q2 = Q3
   df['tsSkew'] = df[['tsLow', 'tsMid', 'tsHigh', 'tsBowleyNum','tsBowleyDen']].apply(
         lambda x: x['tsBowleyNum'] / x['tsBowleyDen'] if x['tsBowleyDen'] !=0 and x['tsMid'] != x['tsLow'] and x['tsMid'] != x['tsHigh'] !=0 else 0.0, axis=1
         )
   df['tsMadm'] = df['delta_time'].apply(lambda x: np.median(np.absolute(np.array(x) - np.median(np.array(x)))))
   df['tsConnDiv'] = df[f_timestamp].apply(lambda x: (x[-1].to_pydatetime() - x[0].to_pydatetime()).seconds / 90)

   # Time delta score calculation
   df['tsConnCountScore'] = df.apply(lambda x: 0.0 if x['tsConnDiv'] == 0  else x['ConnectionCount'] / x['tsConnDiv'] if x['ConnectionCount'] / x['tsConnDiv'] < 1.0 else 1.0 , axis=1)
   df['tsSkewScore'] = 1.0 - abs(df['tsSkew'])
   df['tsMadmScore'] = df['tsMadm'].apply(lambda x: 0 if 1.0 - (x / 30.0) < 0 else 1.0 - (x / 30.0))
   df['tsScore'] = (((df['tsSkewScore'] + df['tsMadmScore'] + df['tsConnCountScore']) / 3.0) * 1000) / 1000

   df['dsLow'] = df[f_sent_bytes].apply(lambda x: np.percentile(np.array(x), 25))
   df['dsMid'] = df[f_sent_bytes].apply(lambda x: np.percentile(np.array(x), 50))
   df['dsHigh'] = df[f_sent_bytes].apply(lambda x: np.percentile(np.array(x), 75))
   df['dsBowleyNum'] = df['dsLow'] + df['dsHigh'] - 2 * df['dsMid']
   df['dsBowleyDen'] = df['dsHigh'] - df['dsLow']


   # dsSkew should equal zero if the denominator equals zero
   # bowley skew is unreliable if Q2 = Q1 or Q2 = Q3
   df['dsSkew'] = df[['dsLow','dsMid','dsHigh','dsBowleyNum','dsBowleyDen']].apply(
         lambda x: x['dsBowleyNum'] / x['dsBowleyDen'] if x['dsBowleyDen'] != 0 and x['dsMid'] != x['dsLow'] and x['dsMid'] != x['dsHigh'] else 0.0, axis=1
         )
   df['dsMadm'] = df[f_sent_bytes].apply(lambda x: np.median(np.absolute(np.array(x) - np.median(np.array(x)))))


   # Data size score calculation of sent bytes
   df['dsSkewScore'] = 1.0 - abs(df['dsSkew'])
   df['dsMadmScore'] = df['dsMadm'].apply(lambda x: 0 if x/ 128.0 < 0 else x/ 128.0)
   df['dsSmallnessScore'] = df['dsMid'].apply(lambda x: 0 if 1.0 - x / 8192.0 < 0 else 1.0 - x / 8192.0)
   df['dsScore'] = (((df['dsSkewScore'] + df['dsMadmScore'] + df['dsSmallnessScore']) / 3.0) * 1000) / 1000

   # Overal Score calculation
   df['Score'] = (df['dsScore'] + df['tsScore']) / 2

   df.sort_values(by= 'Score', ascending=False, inplace=True)
   return df[dst_ip].values.tolist()


