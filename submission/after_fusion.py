from time import time, sleep
from queue import deque
import threading
from matplotlib import pyplot as plt

buffer1 = deque()
lock1 = threading.Lock()
fused_thread_finished = False
fused_thread_finished_lock = threading.Lock()

buffer2 = deque()
lock2 = threading.Lock()

input_thread_finished = False
input_thread_finished_lock = threading.Lock()

dataset_path = "./datasets/library_records_origin/Checkouts_By_Title_Data_Lens_"
years = [str(i) for i in range(2004, 2005)]
timestamps = []

def func_input():
    for year in years:
        with open(dataset_path+year+'.csv', "r") as f:
            for line in f:
                line = tuple((tuple(line.strip().split(",")), [0, 0, 0]))
                while True:
                    lock1.acquire()
                    try:
                        if len(buffer1) < 100:
                            line[1][0] = time()
                            buffer1.appendleft(line)
                            break
                    finally:
                        lock1.release()

    print("func_input almost ended")

    input_thread_finished_lock.acquire()
    try:
        global input_thread_finished
        input_thread_finished = True
    finally:
        input_thread_finished_lock.release()

    print("func_input ended")


def func_fused():
    flag_fused = True
    while flag_fused:
        in_data = None
        lock1.acquire()
        try:
            if buffer1:
                in_data = buffer1.pop()
                in_data[1][1] = time()
        finally:
            lock1.release()

        if in_data and in_data[0][2] == 'jcbk' and in_data[0][3] == 'ncpic':
            lock2.acquire()
            try:
                sleep(0.1)
                in_data[1][2] = time()
                buffer2.appendleft(in_data)
                if len(buffer2) % 100 == 0:
                    print(len(buffer2))
            finally:
                lock2.release()

        else:
            input_thread_finished_lock.acquire()
            try:
                if input_thread_finished:
                    flag_fused = False
            finally:
                input_thread_finished_lock.release()

    print("func_fused almost ended")

    fused_thread_finished_lock.acquire()
    try:
        global fused_thread_finished
        fused_thread_finished = True
    finally:
        fused_thread_finished_lock.release()

    print("func_fused ended")


if __name__ == "__main__":
    input_thread = threading.Thread(target=func_input)
    fused_thread = threading.Thread(target=func_fused)
    init_time = time()
    input_thread.start()
    fused_thread.start()
    input_thread.join()
    fused_thread.join()
    while True:
        fused_thread_finished_lock.acquire()
        try:
            if fused_thread_finished:
                # print("plot begin")
                # plt.plot(timestamps)
                # plt.show()
                # plt.savefig("fusion_result.png")
                x = []
                y = []
                for data in buffer2:
                    data = data[1]
                    x.append(float(data[1]-data[0])/float(data[2]-data[1]))
                    y.append(1.0 / float(data[2]-data[0]))
                plt.scatter(x, y)
                plt.xlabel("operator cost / communication cost")
                plt.ylabel("throughput")
                plt.show()

                break

        finally:
            fused_thread_finished_lock.release()
