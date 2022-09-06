# !/usr/bin/env python

"""
Copyright 2021 Robert McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
from glob import glob
import pandas as pd
import geopandas as gpd
from datetime import timedelta
import numpy as np

warnings.filterwarnings("ignore")


def compare_dataframes_fn(previous, input_data_gdf):
    # (prop_currency_transition_df, prop_currency_test_df)
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print('previous data: ', previous.shape)
    print('input_data_gdf: ', input_data_gdf.shape)
    print('columns_' * 20)
    print('previous data: ', list(previous.columns))
    print('input_data_gdf: ', list(input_data_gdf.columns))

    previous.drop(columns=['DATE_CURR_2'], inplace=True)
    input_data_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    previous.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\previous_line274.csv")
    input_data_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_data_line275.csv")
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_',
                                           on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                               'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE',
                                               'geometry']) # 'STATUS' removed
    input_data_gdf2.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\compared___.csv")
    input_both_df = input_data_gdf2[input_data_gdf2['merge_'] == 'both']
    input_both_df['compare'] = 'both'
    input_both_df.drop(columns=['compare'], inplace=True)
    print('input_both_df-------- before: ', input_both_df.info())
    if 'STATUS_x' in input_both_df.columns:
        input_both_df.drop(columns=['STATUS_x'], inplace=True)
    if 'STATUS_y' in input_both_df.columns:
        input_both_df.drop(columns=['STATUS_y'], inplace=True)
    print('-' * 50)
    print('input_both_df-------- after: ', input_both_df.info())
    #todo new status x and y columns may need deleting before progression (compared___.csv)
    #todo no currency date in the faulty bore

    length_of_both = len(input_both_df)

    # edited_data_df.drop(columns=['merge_'], inplace=True)
    input_both_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_both_df_292.csv")

    return length_of_both


def previous_data_check_fn(transition_dir, year, feature_type):
    """
    Search for previously transferred data and open shapefile as a geo-dataframe.
    :param transition_dir: string object containing the transition directory path.
    :param year: integer object containing the year (XXXX).
    :param feature_type: string object containing the shapefiles geometry.
    :return previous_gdf: geo-dataframe containing the feature specific previously transferred data records.
    """

    print(':' * 50)
    print(' - Checking for previous ', feature_type, ' transition data....')
    print(':' * 50)

    previous_dir_path = os.path.join(transition_dir, str(year), "previous_transfer", feature_type)
    print('previous_dir_path: ', previous_dir_path)

    # migration_overwrite_list = []
    if glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

        for file in glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):
            print(' - located: ', file)
            # open as geo-dataframe
            previous_gdf = gpd.read_file(file, geometry="geometry")

    else:
        previous_gdf = 'BLANK'

    return previous_gdf


def add_date_curr_2_fn(previous_gdf, gdf):
    # convert geo-dataframe to a pandas df
    previous_df = pd.DataFrame(previous_gdf)
    # convert string object to dateTime object
    previous_df['DATE_CURR_2'] = pd.to_datetime(previous_df['DATE_CURR'])
    print('.... located.')

    # convert geo-dataframe to a pandas df
    test_df = pd.DataFrame(gdf)
    # convert string object to dateTime object
    test_df['DATE_CURR_2'] = pd.to_datetime(test_df['DATE_CURR'])

    return test_df, previous_df


def thirty_day_currency_range_fn(currency):

    # create two variables start and end dates to use to filter previous data
    start_date = currency - pd.Timedelta(days=15)
    end_date = currency + pd.Timedelta(days=1)


    return start_date, end_date


def filter_data_currency_fn(start_date, end_date, prop_filter_test_df, prop_filter_transition_df, currency, prop_):
    """

    :param start_date: date time object containing the currency date - n of the test data.
    :param end_date: date time object containing the currency date + n of the test data.
    :param prop_filter_test_df: geo-dataframe containing the new data property  specific.
    :param prop_filter_transition_df: geo-dataframe containing the existing transition data.
    :param currency: date time object containing the currency date of the test data.
    :param prop_: string object containing the property name.
    :return prop_currency_test_df: geo-dataframe object containing the test data that has been filtered on property
    and currency date.
    :return prop_currency_filter_transition_df: geo-dataframe object containing all of the previous property specific
    data in the transition folder between the start and end dates - this data should be removed from the transition
    directory.
    """
    # filter the property filtered test data by currency date.
    print('+'*50)
    print('filtering by: ', prop_)
    print('+' * 50)

    prop_currency_test_df = prop_filter_test_df[prop_filter_test_df['DATE_CURR_2'] == currency]

    prop_currency_test_df.to_csv(
        r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\' + prop_ + "_filtered_test_data.csv")

    prop_currency_filter_transition_df = prop_filter_transition_df[
        (prop_filter_transition_df['PROPERTY'] == prop_) & (prop_filter_transition_df['DATE_CURR_2'] < start_date) &
        (prop_filter_transition_df['DATE_CURR_2'] > end_date)]

    prop_currency_filter_transition_df.to_csv(
        r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\' + prop_ + "_filter_previous_data.csv")

    return prop_currency_test_df, prop_currency_filter_transition_df #, keep_previous_df



def filter_previous_data_latest_currency_date_fn(filter_previous_df, previous_df): #, previous_df):
    """
    Identify the latest date for property specific data win the previously transferred datasets. Then filter the
    previous_df by the latest date.
    :param filter_previous_df:
    :param previous_df:
    :return curr_date_previous_df: property specific previously transferred data filtered by the latest currency date.
    """
    # determine the latest currency date.
    date_list = []
    for i in filter_previous_df.DATE_CURR_2.unique():
        date_list.append(i)
        # todo check that this actually works by adding fake data to the shapefile
    print(' Checking for the latest property specific currency date of the previous data......')
    latest_date = (max(date_list))
    print(" - Latest_date: ", latest_date)

    # filter the dataframe by the latest recorded curr_date
    curr_date_previous_df = previous_df[previous_df['DATE_CURR_2'] == latest_date] # used to be previous_df

    return curr_date_previous_df


def filter_non_properties_fn(gdf, prop_list):
    print('filter_non_properties_fn started....')

    list_b = []
    print('test data prop list: ', prop_list)

    transition_data_list_a = gdf.PROPERTY.unique().tolist()
    print('prop names in transition data: ', transition_data_list_a)

    for prop_ in prop_list:

        prop_gdf = gdf[gdf['PROPERTY'] == prop_]
        if len(prop_gdf.index) > 0:
            list_b.append(prop_gdf)
            print('property identified: ', prop_)

    return list_b


def other_properties_compare_dataframes_fn(original_transition_gdf, all_prop_test_gdf):



    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """

    merge_gdf = original_transition_gdf.merge(all_prop_test_gdf, how='outer', indicator='merge_')
    output_gdf = merge_gdf[merge_gdf['merge_'] == 'left_only']
    output_gdf['compare'] = 'left_only'
    print('-' * 50)
    output_gdf.drop(columns=['merge_'], inplace=True)

    left_gdf = output_gdf[output_gdf['compare'] == 'left_only']
    left_gdf.drop(columns=['compare'], inplace=True)

    if len(left_gdf.index) > 0:
        output_status = True
        # edited_data_df.drop(columns=['merge_'], inplace=True)
        left_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\left_gdf_line373.csv")
    else:
        output_status = False
        left_gdf = None

    return left_gdf, output_status


def other_properties_compare_dataframes______fn(original_transition_gdf, all_prop_test_gdf):

    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe
    object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only
    preoperty specific data that has a currency date before or after the strat and end times respectively.

    """

    print('H'*50)
    date_curr_list = original_transition_gdf.DATE_CURR.unique().tolist()
    print('original_transition_gdf curr list: ', date_curr_list)

    date_curr_list = all_prop_test_gdf.DATE_CURR.unique().tolist()
    print('all_prop_test_gdf curr list: ', date_curr_list)

    print('H' * 50)
    if 'DATE_CURR_2' in original_transition_gdf.columns:
        original_transition_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    if 'DATE_CURR_2' in all_prop_test_gdf.columns:
        all_prop_test_gdf.drop(columns=['DATE_CURR_2'], inplace=True)


    original_transition_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\original_transition_gdf.csv")

    all_prop_test_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\all_prop_test_gdf.csv")

    merge_gdf = original_transition_gdf.merge(all_prop_test_gdf, how='outer', indicator='merge_')
    output_gdf = merge_gdf[merge_gdf['merge_'] == 'both']
    output_gdf['compare'] = 'both'
    print('-' * 50)
    output_gdf = merge_gdf
    output_gdf.drop(columns=['merge_'], inplace=True)


    '''left_gdf = output_gdf[output_gdf['compare'] == 'left_only']
    left_gdf.drop(columns=['compare'], inplace=True)'''

    if len(output_gdf.index) > 0:
        output_status = True
        # edited_data_df.drop(columns=['merge_'], inplace=True)
        output_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\left_output_gdf_line373.csv")
    else:
        output_status = False
        output_gdf = None

    return output_gdf, output_status


def filter_properties_start_end_dates_fn(gdf, prop_list, start_date, end_date):

    print('------------------- filter_properties_start_end_dates_fn ----------------------')

    gdf.to_csv(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\original_transition_gdf')




    list_c = []
    gdf['DATE_CURR_2'] = pd.to_datetime(gdf['DATE_CURR'])

    for prop_ in prop_list:
        print('prop_list: ', prop_list)
        print('less then start date: ', start_date)

        prop_before_gdf = gdf[(gdf['PROPERTY'] == prop_) & (gdf['DATE_CURR_2'] < start_date)]
        prop_before_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

        print('prop_before_gdf: ', prop_before_gdf)

        date_curr_list = prop_before_gdf.DATE_CURR.unique().tolist()
        print('prop_before_gdf date curr list: ', date_curr_list)

        if len(prop_before_gdf) > 0:
            list_c.append(prop_before_gdf)

        """print('greater then start date: ', end_date)
        prop_after_gdf = gdf[(gdf['PROPERTY'] == prop_) & (gdf['DATE_CURR_2'] > end_date)]
        prop_after_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

        print('prop_after_gdf: ', prop_after_gdf)

        date_curr_list = prop_after_gdf.DATE_CURR.unique().tolist()
        print('prop_before_gdf date curr list: ', date_curr_list)

        if len(prop_after_gdf) > 0:
            list_c.append(prop_after_gdf)"""

    return list_c


def check_dataframe_or_geo_dataframe_fn(previous_transition_list):
    """
    Check if the list elements are pandas dataframes or geopandas geo-dataframes. Convert geo-dataframes to pd
    dataframes and append to df_list and concatenate to one dataframe.
    :param previous_transition_list: list object containing  pandas dataframes and geo-dataframes.
    :return df: concatenated pandas dataframe from df_list.
    """
    df_list = []
    for i in previous_transition_list:
        if isinstance(i, gpd.GeoDataFrame):
            print('Convert to pd DataFrame')
            df = pd.DataFrame(i)
            df_list.append(df)

        elif isinstance(i, pd.DataFrame):
            print('Is already a pd DataFrame')
            df_list.append(i)

        else:
            print("!!!!! - not considered a geo-dataframe")

    df = pd.concat(df_list)

    return df

def convert_to_geo_df_remove_date_curr_2_fn(df):
    # convert concatenated df to geo-dataframe
    final_gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # delete DATE_CURR_2 from column headers
    if 'DATE_CURR_2' in final_gdf.columns:
        del final_gdf['DATE_CURR_2']

    return final_gdf


def main_routine(gdf, feature_type, transition_dir, year, status_text, assets_dir, pastoral_estate,
                 pastoral_districts_path):
    """

    """
    print('1_5__' * 50)
    print('step1_5 initiated')
    print('working on: ', feature_type)
    print('1_5__' * 50)

    previous_transition_list = []
    final_transition_list = []

    keep_previous_list = []
    delete_previous_list = []

    list_a = []

    previous_dir_path = os.path.join(transition_dir, str(year), "previous_transfer", feature_type)
    print('previous_dir_path: ', previous_dir_path)
    print('+'*50)
    print("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp"))

    for file__ in glob("{0}\\{1}".format(previous_dir_path, "*")):
        print('file__', file__)


    if glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

        for file in glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):
            print(' - located: ', file)
            # open as geo-dataframe
            transition_gdf = gpd.read_file(file, geometry="geometry")
            print(';;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;')
            transition_data_list_a = transition_gdf.PROPERTY.unique().tolist()
            print('prop names in transition_gdf : ', transition_data_list_a)

            original_transition_gdf_status = True
            original_transition_gdf = transition_gdf.copy()

            transition_data_list_a = original_transition_gdf.PROPERTY.unique().tolist()
            print('prop names in original_transition_gdf : ', transition_data_list_a)

            test_df, transition_df = add_date_curr_2_fn(transition_gdf, gdf)

            # for loop through test dataframe based on property name.
            prop_list = test_df.PROPERTY.unique().tolist()

            for prop_ in prop_list:
                print(' - Property name: ', prop_)
                # filter dataframe based on property name.
                prop_filter_test_df = test_df[test_df['PROPERTY'] == prop_]
                prop_filter_transition_df = transition_df[transition_df['PROPERTY'] == prop_]

                if len(prop_filter_test_df.index) > 0:
                    print(' - Number of observations: ', len(prop_filter_test_df.index))

                    # for loop through filtered test dataframe based on currency date.
                    for currency in prop_filter_test_df.DATE_CURR_2.unique():
                        print('CURRENCY: ', currency)
                        # calculate + and - 15 days from today's date (currency date).
                        start_date, end_date = thirty_day_currency_range_fn(currency)

                        print(' - Currency date: ', currency)
                        print('Checking for dates between:')
                        print(' - Start_date: ', start_date)
                        print(' - End_date: ', end_date)

                        # filter test data by currency value in the for loop.
                        prop_currency_test_df, prop_currency_transition_df = filter_data_currency_fn(
                            start_date, end_date, prop_filter_test_df, prop_filter_transition_df, currency, prop_)

                        if len(prop_currency_transition_df.index) == len(prop_currency_test_df.index):
                            print('test and transit data is the same (i.e. EXISTING DATA.')
                            prop_currency_transition_df['NEW_DELETE'] = 1  # DELETE
                            list_a.append(prop_currency_transition_df)
                            prop_currency_test_df['NEW_DELETE'] = 0  # KEEP
                            list_a.append(prop_currency_test_df)

                        else:
                            print('test and transit data are NOT the same (i.e. NEW DATA.')
                            print('test data should be kept and transit data deleted....')

                            prop_currency_transition_df['NEW_DELETE'] = 1  # DELETE
                            list_a.append(prop_currency_transition_df)
                            prop_currency_test_df['NEW_DELETE'] = 0  # KEEP
                            list_a.append(prop_currency_test_df)


                else:
                    print('No test data for the property: ', prop_)
    else:

        original_transition_gdf_status = False
        print('No previous data exists.....')
        #gdf['DELETE'] = 0 # KEEP
        gdf['NEW_DELETE'] = 0 # KEEP
        list_a.append(gdf)

    if len(list_a) > 0:
        print('list_a is greater than 0')

        # check that
        keep_df = check_dataframe_or_geo_dataframe_fn(list_a)
        final_test_gdf = convert_to_geo_df_remove_date_curr_2_fn(keep_df)
        final_test_gdf_status = True

    else:
        final_test_gdf = None
        final_test_gdf_status = False


    # ----------------------------------- TRANSITION DATA OUTSIDE OF FILTERS -------------------------------------------
    # --------------------------------------------- other properties ---------------------------------------------------

    print('--------------------- original_transition_gdf_status -------: ', original_transition_gdf_status)

    if original_transition_gdf_status:

        transition_data_list_a = original_transition_gdf.PROPERTY.unique().tolist()
        print('prop names in transition data: ', transition_data_list_a)


        # extract all previous transition data for properties not included in the test data.
        list_b = filter_non_properties_fn(original_transition_gdf, prop_list)

        transition_data_list_a = original_transition_gdf.PROPERTY.unique().tolist()
        print('prop names in original_transition_gdf: ', transition_data_list_a)


        print('list_b: ', list_b)
        if len(list_b) > 0:
            print('list_b is greater than 0')
            all_prop_test_gdf = pd.concat(list_b)
            print('all_prop_test_gdf: ', all_prop_test_gdf)

            transition_data_list_a = all_prop_test_gdf.PROPERTY.unique().tolist()
            print('prop names in all_prop_test_gdf: ', transition_data_list_a)


            # extract all other properties within the transition dataset that do not match the test data regardless of the
            # currency date.
            other_properties_gdf, other_properties_output_status = other_properties_compare_dataframes_fn(
                original_transition_gdf, all_prop_test_gdf)

            if other_properties_output_status:
                transition_data_list_a = other_properties_gdf.PROPERTY.unique().tolist()
                print('; 627 - ' * 20)
                print('prop names in other_properties_gdf: ', transition_data_list_a)

        elif len(list_b) == 0:

            other_properties_gdf = original_transition_gdf
            other_properties_output_status = True

            transition_data_list_a = other_properties_gdf.PROPERTY.unique().tolist()
            print('; 637 -' * 20)
            print('prop names in other_properties_gdf: ', transition_data_list_a)



        else:
            #todo possible error here
            other_properties_gdf = None
            other_properties_output_status = False

    else:
        original_transition_gdf = None
        original_transition_gdf_status = False
        other_properties_gdf = None
        other_properties_output_status = False


    # ---------------------------------- same properties before and after end date -------------------------------------

    ''' Extract all observations that occurred bare and after the start and end dates respectively. 
    These are property specific.'''

    if original_transition_gdf_status:
        # extract all previous transition data for properties included in the test data.
        list_c = filter_properties_start_end_dates_fn(original_transition_gdf, prop_list, start_date, end_date)

        if len(list_c) > 0:
            print('list_c is greater than 0')
            all_prop_start_end_dates_test_gdf = pd.concat(list_c)
            print('LENGTH - all_prop_start_end_dates_test_gdf: ', len(all_prop_start_end_dates_test_gdf.index))
            all_prop_start_end_dates_test_gdf.to_csv(
                r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\all_prop_start_end_dates_test_gdf.csv')

            date_curr_list = all_prop_start_end_dates_test_gdf.DATE_CURR.unique().tolist()
            print('all_prop_start_end_dates_test_gdf: ', date_curr_list)



            # extract all previous transaction data for matching properties before start date and after end date.
            prop_start_end_dates_gdf, prop_start_end_dates_output_status = other_properties_compare_dataframes______fn(
                original_transition_gdf, all_prop_start_end_dates_test_gdf)

            print('prop_start_end_dates_gdf: ', len(prop_start_end_dates_gdf.index))


        else:
            print('list_c is 0')
            prop_start_end_dates_gdf = None
            prop_start_end_dates_output_status = False


    else:
        original_transition_gdf = None
        original_transition_gdf_status = False
        prop_start_end_dates_gdf = None
        prop_start_end_dates_output_status = False


    print('---------------- END 1_5 ----------------')

    return final_test_gdf, final_test_gdf_status, status_text, other_properties_gdf, other_properties_output_status, prop_start_end_dates_gdf, \
           prop_start_end_dates_output_status, original_transition_gdf, original_transition_gdf_status


if __name__ == '__main__':
    main_routine()
