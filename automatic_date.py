import argparse
import collections
import os.path as osp

import pandas as pd

digit_dict = {1: "一", 2: "二", 3: "三", 4: "四", 5: "五", 6: "六", 7: "日"}


def config_args():
    parser = argparse.ArgumentParser('time')
    parser.add_argument("--data_root", type=str,
                        default="D:\\QYZ\\Code\\misc\\appendix",
                        help="root path of source data")
    parser.add_argument("--output_root", type=str,
                        default="D:\\QYZ\\Code\\misc\\result",
                        help="root path of source data")
    parsers = parser.parse_args()
    return parsers


def time_transport(time):
    year = pd.to_datetime(time).year
    month = pd.to_datetime(time).month
    day = pd.to_datetime(time).day
    week = pd.to_datetime(time).week
    weekday = "周" + digit_dict[pd.to_datetime(time).weekday() + 1]
    time_str = '/'.join([str(year), str(month), str(day)])
    return year, f"第{week}周", weekday, time_str


def calculate_diff_color(color_series):
    color_series = color_series.values
    # set up count var and init var
    count, init_color = 0, color_series[0]
    # traversing to find different colors
    for color in color_series:
        if color != init_color:
            count += 1
        init_color = color

    return count


def read_xlsx():
    # set up data path
    data_filename = osp.join(args.data_root, "P1_周-日_demo.xlsx")
    # read xlsx file by pandas library
    data_pd = pd.read_excel(data_filename, sheet_name="P1_订单周_demo", skiprows=1)
    data_pd.rename(columns={data_pd.columns[0]: 'index'}, inplace=True)
    # get time sequence
    date_series = set(data_pd['日期'])
    # divide DataFrame by date
    for date in date_series:
        # date conversion
        year, week, weekday, date_str = time_transport(date)
        # get index of DataFrame for date
        data_pd_by_date_index = data_pd[data_pd['日期'] == date_str].index
        # obtain the daily output of the day
        daily_output = data_pd.loc[data_pd_by_date_index, '日产量'].max()
        # calculate daily color change times
        daily_color_change_times = calculate_diff_color(data_pd.loc[data_pd_by_date_index, '喷涂颜色'])
        # daily same-color continuous spraying rate
        daily_same_color_rate = daily_output / (daily_color_change_times + 1e-10)
        # write year to DataFrame
        data_pd.loc[data_pd_by_date_index, '年份'] = year
        # write total daily traffic
        data_pd.loc[data_pd_by_date_index, '日过车总数'] = daily_output
        # write daily color change times
        data_pd.loc[data_pd_by_date_index, '日换色次数'] = daily_color_change_times
        # write daily same-color continuous spraying rate
        data_pd.loc[data_pd_by_date_index, '日同色连喷率'] = daily_same_color_rate
        # write week to DataFrame
        data_pd.loc[data_pd_by_date_index, '工作周'] = week
        # write weekday to DataFrame
        data_pd.loc[data_pd_by_date_index, '周几'] = weekday
        # store data for same week
        week_data_dict[week].append(dict(
            date=date_str,
            daily_output=daily_output,
            daily_color_change_times=daily_color_change_times
        ))

    # traverse every work week
    for week, data_list in week_data_dict.items():
        # get index of DataFrame for week
        data_pd_by_week_index = data_pd[data_pd['工作周'] == week].index
        # static output of the week and color change times
        week_output, week_color_change_times = 0, 0
        for data_day in data_list:
            week_output += data_day["daily_output"]
            week_color_change_times += data_day["daily_color_change_times"]
        # compute week same-color continuous spraying rate
        week_same_color_rate = week_output / (week_color_change_times + 1e-10)
        # write total daily traffic
        data_pd.loc[data_pd_by_week_index, '周过车总数'] = week_output
        # write daily color change times
        data_pd.loc[data_pd_by_week_index, '周换色次数'] = week_color_change_times
        # write daily same-color continuous spraying rate
        data_pd.loc[data_pd_by_week_index, '周连喷率'] = week_same_color_rate

    # modify data type
    data_pd['年份'].astype('int')
    data_pd['日过车总数'].astype('int')
    data_pd['日换色次数'].astype('int')
    data_pd['日同色连喷率'].astype('float')
    data_pd['周过车总数'].astype('int')
    data_pd['周换色次数'].astype('int')
    data_pd['周连喷率'].astype('float')

    data_pd.to_excel(osp.join(args.output_root, "P1_周-日_output.xlsx"), sheet_name="P1_订单周", index=False)


def main():
    read_xlsx()


if __name__ == "__main__":
    # config list
    args = config_args()
    # store data for the same work week
    week_data_dict = collections.defaultdict(list)

    main()
