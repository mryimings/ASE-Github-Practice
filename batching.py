# This is a modification on local machine after git pull

from time import time
from pyspark import SparkContext

small_path = './datasets/library_records_splitted/'
large_path = './datasets/library_records_origin/Checkouts_By_Title_Data_Lens_'

years = [str(i) for i in range(2005, 2018)]

ItemType_Target = 'jcbk'
Collection_Target = 'ncpic'
latency_large_batch = 0.0
latency_small_batch = 0.0
iteration_step = 1

sc = SparkContext()

for _ in range(iteration_step):

    for year in years:
        data = sc.textFile(large_path+year+".csv").map(lambda x: tuple(x.strip().split(',')[2:4]))
        start_time = time() * 1000
        data = data.filter(lambda x: x[0] == ItemType_Target and x[1] == Collection_Target)
        end_time = time() * 1000
        latency_large_batch += (end_time - start_time)

    for i in range(178):
        data = sc.textFile(str(i)+ ".csv").map(lambda x: tuple(x.strip().split(',')[2:4]))
        start_time = time() * 1000
        data = data.filter(lambda x: x[0] == ItemType_Target and x[1] == Collection_Target)
        end_time = time() * 1000
        latency_small_batch += (end_time - start_time)

print("latency_before_fusion", latency_large_batch)
print("latency_after_fusion", latency_small_batch)