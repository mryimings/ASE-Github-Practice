from time import time, sleep
from queue import deque
import threading
from matplotlib import pyplot as plt

input_thread_finished = False
input_thread_finish_lock = threading.Lock()

buffer1 = deque()
lock1 = threading.Lock()

thread_A_finished = False
thread_A_finished_lock = threading.Lock()

buffer2 = deque()
lock2 = threading.Lock()

thread_B_finished = False
thread_B_finished_lock = threading.Lock()

buffer3 = deque()
lock3 = threading.Lock()

dataset_path = "./datasets/library_records_origin/Checkouts_By_Title_Data_Lens_"
years = [str(i) for i in range(2004, 2005)]
timestamps = []

def func_input():
    for year in years:
        with open(dataset_path+year+'.csv', "r") as f:
            for line in f:
                line = tuple((line.strip().split(","), [0, 0, 0, 0, 0]))
                while True:
                    lock1.acquire()
                    try:
                        if len(buffer1) < 100:
                            line[1][0] = time()
                            buffer1.appendleft(line)
                            break
                    finally:
                        lock1.release()

    input_thread_finish_lock.acquire()
    try:
        global input_thread_finished
        input_thread_finished = True
    finally:
        input_thread_finish_lock.release()

    print("func_input ended")


def func_A():
    flag_A = True
    while flag_A:
        in_data = None
        lock1.acquire()
        try:
            if buffer1:
                in_data = buffer1.pop()
                in_data[1][1] = time()
        finally:
            lock1.release()

        if in_data and in_data[0][2] == 'jcbk':
            lock2.acquire()
            try:
                sleep(0.01)
                in_data[1][2] = time()
                buffer2.appendleft(in_data)
                if len(buffer2) % 100 == 0:
                    print("len buffer2", len(buffer2))
            finally:
                lock2.release()

        else:
            input_thread_finish_lock.acquire()
            try:
                if input_thread_finished:
                    flag_A = False
            finally:
                input_thread_finish_lock.release()

        # print(count)

    print("func_fuse almost ended")

    thread_A_finished_lock.acquire()
    try:
        global thread_A_finished
        thread_A_finished = True
    finally:
        thread_A_finished_lock.release()

    print("func_fused ended")


def func_B():
    flag_B = True
    while flag_B:
        in_data = None
        lock2.acquire()
        try:
            if buffer2:
                in_data = buffer2.pop()
                in_data[1][3] = time()
        finally:
            lock2.release()

        if in_data and in_data[0][3] == 'ncpic':
            lock3.acquire()
            try:
                sleep(0.01)
                in_data[1][4] = time()
                buffer3.appendleft(in_data)
                if len(buffer3) % 100 == 0:
                    print("len buffer3:", len(buffer3))
            finally:
                lock3.release()

        else:
            thread_A_finished_lock.acquire()
            try:
                if thread_A_finished:
                    flag_B = False
            finally:
                thread_A_finished_lock.release()

    print("func_fuse almost ended")

    thread_B_finished_lock.acquire()
    try:
        global thread_B_finished
        thread_B_finished = True
    finally:
        thread_B_finished_lock.release()

    print("func_fused ended")


if __name__ == "__main__":
    thread_input = threading.Thread(target=func_input)
    thread_A = threading.Thread(target=func_A)
    thread_B = threading.Thread(target=func_B)
    init_time = time()
    thread_input.start()
    thread_A.start()
    thread_B.start()
    thread_input.join()
    thread_A.join()
    thread_B.join()
    while True:
        thread_B_finished_lock.acquire()
        try:
            if thread_B_finished:
                x = []
                y = []
                for data in buffer3:
                    print(data)
                    data = data[1]
                    x.append(float(data[1] - data[0] + data[3] - data[2]) / float(data[2] - data[1] + data[4] - data[3]))
                    y.append(1.0 / float(data[4] - data[0]))
                print(len(x), len(y))
                plt.scatter(x, y)
                plt.xlabel("operator cost / communication cost")
                plt.ylabel("throughput")
                plt.show()
                break

        finally:
            thread_B_finished_lock.release()