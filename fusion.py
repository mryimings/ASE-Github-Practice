from time import time
from pyspark import SparkContext

sc = SparkContext()

years = [str(i) for i in range(2005, 2018)]

ItemType_Target = 'jcbk'
Collection_Target = 'ncpic'
dataset_path = "./datasets/library_records_origin/Checkouts_By_Title_Data_Lens_"
latency_before_fusion = 0.0
latency_after_fusion = 0.0
iteration_step = 100

for _ in range(iteration_step):

    for year in years:
        data = sc.textFile(dataset_path+year+".csv").map(lambda x: tuple(x.strip().split(',')[2:4]))
        start_time = time() * 1000
        data = data.filter(lambda x: x[0] == ItemType_Target)
        data = data.filter(lambda x: x[1] == Collection_Target)
        end_time = time() * 1000
        latency_before_fusion += (end_time - start_time)

    for year in years:
        data = sc.textFile(dataset_path + year + ".csv").map(lambda x: tuple(x.strip().split(',')[2:4]))
        start_time = time() * 1000
        data = data.filter(lambda x: x[0] == ItemType_Target and x[1] == Collection_Target)
        end_time = time() * 1000
        latency_after_fusion += (end_time - start_time)


print("latency_before_fusion", latency_before_fusion)
print("latency_after_fusion", latency_after_fusion)
