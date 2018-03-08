dataset_path = "./datasets/library_records_origin/Checkouts_By_Title_Data_Lens_"

with open(dataset_path+'2005.csv', "r") as f_read:
    with open(dataset_path+"2004.csv", "w") as f_write:
        count = 0
        for line in f_read:
            f_write.write(line)
            count += 1
            if count >= 100000:
                break

