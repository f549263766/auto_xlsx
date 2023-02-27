import os
import re
import os.path as osp
import pandas as pd
import argparse
import datetime
import tqdm
from concurrent import futures


def config_args():
    parser = argparse.ArgumentParser('time')
    parser.add_argument("--data_root", type=str,
                        default="D:\\QYZ\\Code\\misc\\data_2",
                        help="root path of source data")
    parser.add_argument("--output_root", type=str,
                        default="D:\\QYZ\\Code\\misc\\result",
                        help="root path of source data")
    parser.add_argument("--num_thread", type=int,
                        default=10,
                        help="the number of multi-thread")
    parsers = parser.parse_args()
    return parsers


def timestamp_to_date(time_stamp):
    if 10 < len(str(time_stamp)) < 15:
        k = len(str(time_stamp)) - 10
        timestamp = datetime.datetime.fromtimestamp(time_stamp / (1 * 10 ** k))
        date_value = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        return -1
    return date_value


def update_date(csv_file):
    csv_data = pd.read_csv(csv_file)
    csv_data.insert(loc=1, column='goaltime', value=0)

    csv_data['goaltime'] = csv_data['time'].apply(lambda x: timestamp_to_date(x))

    if not osp.exists(args.output_root):
        os.mkdir(args.output_root)
    sub_dir = csv_file.replace(osp.basename(csv_file), "").split("\\")[-2]
    output_dir = osp.join(args.output_root, sub_dir)
    if not osp.exists(output_dir):
        os.mkdir(output_dir)
    output_file = osp.join(output_dir, osp.basename(csv_file))
    csv_data.to_csv(output_file, index=False)


def read_file():
    csv_list = [osp.join(args.data_root, sub_dir, file_name) for sub_dir in os.listdir(args.data_root) for file_name in
                os.listdir(osp.join(args.data_root, sub_dir))
                if re.search(r".csv", file_name)]

    # multi-thread
    with futures.ThreadPoolExecutor(max_workers=args.num_thread) as t:
        task_list = [t.submit(update_date, csv_file) for csv_file in csv_list]
        for task in tqdm.tqdm(futures.as_completed(task_list), total=len(task_list)):
            task.result()


def main():
    read_file()


if __name__ == "__main__":
    args = config_args()
    main()
