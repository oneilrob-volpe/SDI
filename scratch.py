import os

# DATA_FOLDER = r'C:\Users\robert.oneil.ctr\Documents\projects\OTS-P Data Fusion\data\waze_IN'
DATA_FOLDER = r'C:\Users\robert.oneil.ctr\Documents\projects\OTS-P Data Fusion\data\subset'

def main():
    line_count = 0
    file_count = 0
    for root, dirs, file_names in os.walk(DATA_FOLDER):
        for file_name in file_names:
            file_path = os.path.join(DATA_FOLDER, file_name)
            file_count += 1
            with open(file_path, "r") as data_file:
                for i, l in enumerate(data_file):
                    pass
                line_count += i

    print(file_count, line_count)

if __name__ == "__main__":
    main()
