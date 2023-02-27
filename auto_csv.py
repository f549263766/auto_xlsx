import argparse
import collections
import os
import os.path as osp
import re

import numpy as np
import pandas as pd
from openpyxl import Workbook


def config_args():
    parser = argparse.ArgumentParser('time')
    parser.add_argument("--data_root", type=str,
                        default="D:\\QYZ\\Code\\misc\\data",
                        help="root path of source data")
    parser.add_argument("--output_root", type=str,
                        default="D:\\QYZ\\Code\\misc\\result",
                        help="root path of source data")
    parser.add_argument("--data_index_file", type=str,
                        default="D:\\QYZ\\Code\\misc\\index\\137J-4 数采清单V1.0.xlsx",
                        help="root path of source data")
    parsers = parser.parse_args()
    return parsers


def read_data_acquisition_list(data_path):
    data_pd = pd.read_excel(data_path, sheet_name="数采清单 V1.0", skiprows=4)
    data_pd = data_pd.dropna(axis=0, how='any', subset=['采集项编码'])
    data_pd = data_pd.fillna(method='ffill')
    data_np = np.array(data_pd.values)
    index_head = np.asarray(data_pd.columns)[2:13]
    index_data = data_np[:, 2:13]

    for index, item in enumerate(index_head):
        index_to_data_dict[item] = index_data[:, index]

    repeat_count = collections.defaultdict(int)
    for index, item in enumerate(zip(index_to_data_dict['产线'], index_to_data_dict['设备'])):
        key_name = '-'.join(item)
        count = ''
        if key_name in device_index.keys():
            if key_name in repeat_count.keys():
                count = str(repeat_count[key_name])
            if abs(device_index[key_name + count][-1] - index) > 1:
                repeat_count[key_name] += 1
                count = str(repeat_count[key_name])
        device_index[key_name + count].append(index)


def read_csv_index():
    sub_dir_list = [osp.join(args.data_root, sub_dir) for sub_dir in os.listdir(args.data_root)]
    csv_list = [osp.join(args.data_root, sub_dir, csv_file) for sub_dir in sub_dir_list for csv_file in
                os.listdir(sub_dir) if re.search(r".csv", csv_file)]

    for csv_file in csv_list:
        csv_basename = osp.basename(csv_file)
        filename_to_path[csv_basename].append(csv_file)


def write_data_csv():
    wb = Workbook()

    # 遍历设备
    for sheet_idx, (device_name, device_index_list) in enumerate(device_index.items()):
        # 设置表头
        ws_header = ["时间", "产线", "工站", "设备"]
        # 激活sheet
        ws = wb.worksheets[sheet_idx]
        # 采集项编码遍历范围
        st, ed = device_index_list[0], device_index_list[-1]
        # 获取产线、工站
        production_line, station = index_to_data_dict['产线'][st], index_to_data_dict['工站'][st]
        # 存储变量值和时间
        store_value_list, store_time_list = [], []
        # 遍历采集项编码
        for idx, collection_name in enumerate(index_to_data_dict['采集项编码'][st:ed + 1]):
            # 获取采集项名称
            collection_item_name = index_to_data_dict['采集项名称'][idx + st]
            # 新增表头名
            ws_header.append(collection_item_name)

            # 临时存储时间和变量值
            temp_time_list, temp_value_list = [], []
            # 遍历不同日期的采集项文件
            csv_filename_list = filename_to_path[collection_name + '.csv']
            for csv_filename in csv_filename_list:
                if osp.exists(csv_filename):
                    goal_time, median_value = [], []
                    csv_data = pd.read_csv(csv_filename, index_col=None)
                    csv_data['goaltime'] = pd.to_datetime(csv_data['goaltime'], format='%Y/%m/%d %H')
                    csv_data['goaltime'] = csv_data['goaltime'].dt.strftime('%Y/%m/%d %H')
                    date_series = sorted(set(csv_data['goaltime']))
                    for date in date_series:
                        csv_data_by_date_index = csv_data[csv_data['goaltime'] == date].index
                        median = csv_data.loc[csv_data_by_date_index, 'median'].median()
                        goal_time.append(date)
                        median_value.append(median)
                    temp_time_list.extend(goal_time)
                    temp_value_list.extend(median_value)
            # 更新存储值
            if temp_time_list:
                store_time_list = temp_time_list
            if temp_value_list:
                store_value_list.append(temp_value_list)

        # 添加表名
        ws.append(ws_header)
        # 更新sheet名
        ws.title = device_name
        # 更新产线、工站和设备列表
        production_line_list = [production_line] * len(store_time_list)
        station_list = [station] * len(store_time_list)
        device_list = [device_name] * len(store_time_list)
        # 将时间、产线、工站和设备列表合并
        data_list = [store_time_list, production_line_list, station_list, device_list]
        # 合并中间值
        data_list.extend(store_value_list)
        # 转置数据
        data_list = np.asarray(data_list).T.tolist()
        # 更新sheet内容
        for row_data in data_list:
            ws.append(row_data)
        # 创建新的sheet
        wb.create_sheet()

    # 输出表格
    wb.save(osp.join(args.output_root, "output.xlsx"))


def main():
    read_data_acquisition_list(args.data_index_file)
    read_csv_index()
    write_data_csv()


if __name__ == "__main__":
    args = config_args()
    index_to_data_dict, filename_to_path, device_index = dict(), collections.defaultdict(list), collections.defaultdict(
        list)

    main()
