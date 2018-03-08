import os

years = [str(i) for i in range(2005, 2018)]

output_path = './datasets/library_records_splitted/'

max_line = 500000

index = 0

for file in os.listdir('./datasets/library_records_origin/'):
    with open("./datasets/library_records_origin/"+file, "r", encoding='windows-1252') as f_read:
        count = 0
        f_write = open(output_path+str(index)+".csv", "w")
        for line in f_read:
            f_write.write(line)
            count += 1
            if count >= max_line:
                count = 0
                f_write.close()
                index += 1
                f_write = open(output_path+str(index)+".csv", "w")

        f_write.close()

    print(file, "is completed")

print("all completed")
