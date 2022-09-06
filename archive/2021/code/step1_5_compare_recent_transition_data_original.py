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
                                               'geometry'])  # 'STATUS' removed
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
    # todo new status x and y columns may need deleting before progression (compared___.csv)
    # todo no currency date in the faulty bore

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
    #previous_df['DATE_CURR_2'] = pd.to_datetime(previous_df['DATE_CURR'])
    #print('.... located.')

    # convert geo-dataframe to a pandas df
    test_df = pd.DataFrame(gdf)
    # convert string object to dateTime object
    #test_df['DATE_CURR_2'] = pd.to_datetime(test_df['DATE_CURR'])

    return test_df, previous_df


def thirty_day_currency_range_fn(currency):
    # create two variables start and end dates to use to filter previous data
    start_date = currency - pd.Timedelta(days=30)
    end_date = currency

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
    print('+' * 50)
    print('filtering by: ', prop_)
    print(' currency: ', currency)
    print(' start_date: ', start_date)
    print(' end_date: ', end_date)
    print('+' * 50)

    prop_currency_test_df = prop_filter_test_df[prop_filter_test_df['TRAN_DATE'] == currency]

    prop_currency_test_df.to_csv(
        r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\' + prop_ + "_filtered_test_data.csv")

    prop_before_transition_df = prop_filter_transition_df[
        (prop_filter_transition_df['PROPERTY'] == prop_) & (prop_filter_transition_df['TRAN_DATE'] < start_date)]

    prop_before_transition_df.to_csv(
        r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\1_5_' + prop_ + "_prop_before_transition_df.csv")

    prop_after_transition_df = prop_filter_transition_df[
        (prop_filter_transition_df['PROPERTY'] == prop_) & (prop_filter_transition_df['TRAN_DATE'] >= start_date)]

    prop_after_transition_df.to_csv(
        r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload" + '\\1_5_' + prop_ + "_prop_after_transition_df.csv")

    return prop_currency_test_df, prop_before_transition_df, prop_after_transition_df  # , keep_previous_df


def filter_previous_data_latest_currency_date_fn(gdf):
    """

    """
    # determine the latest currency date.
    date_list = []
    for i in gdf.TRAN_DATE.unique():
        date_list.append(i)

    latest_date = (max(date_list))
    print(" - Latest_date: ", latest_date)

    # filter the dataframe by the latest recorded curr_date
    filtered_gdf = gdf[gdf['TRAN_DATE'] == latest_date]  # used to be previous_df

    return filtered_gdf


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

    print('H' * 50)
    date_curr_list = original_transition_gdf.DATE_CURR.unique().tolist()
    print('original_transition_gdf curr list: ', date_curr_list)

    date_curr_list = all_prop_test_gdf.DATE_CURR.unique().tolist()
    print('all_prop_test_gdf curr list: ', date_curr_list)

    print('H' * 50)
    if 'DATE_CURR_2' in original_transition_gdf.columns:
        original_transition_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    if 'DATE_CURR_2' in all_prop_test_gdf.columns:
        all_prop_test_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    original_transition_gdf.to_csv(
        r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\original_transition_gdf.csv")

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
    #todo removed date_curr_2 - function now just convert to geo-dataframe
    final_gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # delete DATE_CURR_2 from column headers
    '''if 'DATE_CURR_2' in final_gdf.columns:
        del final_gdf['DATE_CURR_2']'''

    return final_gdf


def add_tran_date_fn(trans_gdf):

    trans_gdf['TRAN_DATE'] = pd.to_datetime(trans_gdf['DATE_CURR'])
    '''date_curr_list = trans_gdf.TRAN_DATE.tolist()
    print(date_curr_list)'''

    return trans_gdf



def main_routine(gdf, feature_type, transition_dir, year, status_text, assets_dir, pastoral_estate,
                 pastoral_districts_path):
    """ 1. Search for feature type specific data already in the transfer directory.

    """

    print('step1_5 initiated  ' * 5)
    print('='*50)
    print('working on: ', feature_type)
    print('=' * 50)
    previous_transition_list = []
    final_transition_list = []

    keep_previous_list = []
    delete_previous_list = []
    new_list = []
    other_list = []
    before_list = []
    after_list = []
    new_prop_list = []

    list_a = []
    print('Searching for ', feature_type, ' data already in the transition directory....')
    previous_dir_path = os.path.join(transition_dir, str(year), "previous_transfer", feature_type)
    status_text.write('\nSearching for previous data in the transition directory....\n')
    if glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

        for file in glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):
            print(' - located: ', file)
            status_text.write(' - located: ')
            status_text.write(file)

            # open as geo-dataframe called transition_gdf
            trans_gdf = gpd.read_file(file, geometry="geometry")

            # add a trans_date field (date_time object) from the date_curr field
            transition_gdf = add_tran_date_fn(trans_gdf)

            # set variable_original_transition_gdf_status to True as transition data exists.
            original_transition_gdf_status = True
            # create a copy of the existing transition geo-dataframe
            original_transition_gdf = transition_gdf.copy()

            # create a list of unique property names within the copied existing transition geo-dataframe.
            transition_data_list_a = original_transition_gdf.PROPERTY.unique().tolist()
            print('prop names in original_transition_gdf : ', transition_data_list_a)

            #todo look into this-- removed curr_date2 just converted to pd dataframe not sure why....
            test_df, transition_df = add_date_curr_2_fn(transition_gdf, gdf)

            # for loop through test dataframe based on property name.
            prop_list = test_df.PROPERTY.unique().tolist()
            status_text.write('\nThe following properties exist within the TEST data set....\n')
            for prop_ in prop_list:
                print(' - Property name: ', prop_)
                status_text.write('\n - filtering by property name\n')
                status_text.write(prop_)
                # filter dataframe based on property name.
                prop_filter_test_df = test_df[test_df['PROPERTY'] == prop_]
                prop_filter_transition_df = transition_df[transition_df['PROPERTY'] == prop_]

                # ensure that property filtered data exists
                if len(prop_filter_test_df.index) > 0:
                    print(' - Number of observations: ', len(prop_filter_test_df.index))

                    # for loop through filtered test dataframe based on currency date.
                    for currency in prop_filter_test_df.TRAN_DATE.unique():
                        status_text.write('\n - filtering by currency date\n')
                        status_text.write(str(currency))
                        print('CURRENCY: ', currency)
                        # calculate + and - 15 days from today's date (currency date).
                        start_date, end_date = thirty_day_currency_range_fn(currency)
                        status_text.write('\n - extracting start (-30) and finish dates (+1)\n')
                        status_text.write('\n  - Start_date: ')
                        status_text.write(str(start_date))
                        status_text.write('\n - End_date: ')
                        status_text.write(str(end_date))

                        print(' - Currency date: ', currency)
                        print('Checking for dates between:')
                        print(' - Start_date: ', start_date)
                        print(' - End_date: ', end_date)

                        # filter test data by currency value in the for loop.

                        # prop_currency_test_df, prop_before_transition_df, prop_after_transition_df

                        '''prop_currency_test_df, prop_currency_transition_df, prop_currency_transition_df_after = \
                            filter_data_currency_fn(
                            start_date, end_date, prop_filter_test_df, prop_filter_transition_df, currency, prop_)'''

                        prop_currency_test_df, prop_before_transition_df, prop_after_transition_df = \
                            filter_data_currency_fn(
                                start_date, end_date, prop_filter_test_df, prop_filter_transition_df, currency, prop_)

                        status_text.write('\n - Filtering transition data by property and start date and property and end date.\n')
                        status_text.write('\n  - property specific transition data observations: ')
                        status_text.write(str(len(prop_filter_transition_df.index)))
                        status_text.write('\n  - property specific after start date observations: ')
                        status_text.write(str(len(prop_after_transition_df.index)))
                        status_text.write('\n  - property specific before start date observations: ')
                        status_text.write(str(len(prop_before_transition_df.index)))
                        print('+'*50)
                        print(':param prop_filter_test_df: geo-dataframe containing the new data property specific.')
                        print(':param prop_filter_transition_df: geo-dataframe containing the existing transition data.')
                        print(':return prop_currency_test_df: geo-dataframe object containing the test data that has been filtered on property and currency date.')
                        print(':return prop_currency_filter_transition_df: geo-dataframe object containing all of the previous property specific data in the transition folder between the start and end dates - this data should be removed from the transition directory.')
                        print('+' * 50)
                        print('prop_currency_test_df: ', len(prop_currency_test_df))
                        print('prop_filter_transition_df: ', len(prop_filter_transition_df))
                        print('prop_after_transition_df: ', len(prop_after_transition_df))
                        print('prop_before_transition_df: ', len(prop_before_transition_df))
                        print('+' * 50)
                        if len(prop_before_transition_df) > 0 and len(prop_currency_test_df) != len(prop_after_transition_df):
                            print('before > 0')
                            before_list.append(prop_before_transition_df)
                            after_list.append(prop_currency_test_df)
                            print('appending before: ',  len(prop_before_transition_df))
                            print('appending after: ', len(prop_currency_test_df))

                        elif len(prop_currency_test_df) == len(prop_after_transition_df):
                            print(" test == after.")
                            after_list.append(prop_currency_test_df)

                            '''elif len(prop_currency_test_df) > len(prop_after_transition_df):
                                print('test < after')
                                after_list.append(prop_currency_test_df)'''

                        elif len(prop_currency_test_df) > 0 and len(prop_filter_transition_df) == 0 and len(prop_after_transition_df) == 0 and len(prop_before_transition_df) == 0:
                            print(" new property")
                            new_prop_list.append(prop_currency_test_df)
                            #todo may nee to append to new_prop_list

                        elif len(prop_currency_test_df) > 0 and len(prop_filter_transition_df) > 0 and len(prop_after_transition_df) > 0 and len(prop_before_transition_df) == 0 and len(prop_currency_test_df) != len(prop_filter_transition_df) :
                            print("existing property - fewer observations")
                            new_list.append(prop_currency_test_df)
                            #todo may nee to append to new_prop_list

                        else:
                            # len(prop_after_transition_df) != len(prop_currency_test_df):
                            print('SCRIPT ERROR---------')
                            print('prop_currency_test_df: ', len(prop_currency_test_df))
                            print('prop_filter_transition_df: ', len(prop_filter_transition_df))
                            print('prop_after_transition_df: ', len(prop_after_transition_df))
                            print('prop_before_transition_df: ', len(prop_before_transition_df))


                            '''print('prop before - no data')
                            after_list.append(prop_currency_test_df)
                            #todo have appended data here
                            after_start_date_gdf = prop_currency_test_df.append(prop_currency_test_df)
                            #todo sort data to latest TRAN DATE
                            final_gdf = filter_previous_data_latest_currency_date_fn(after_start_date_gdf)
                            after_list.append(final_gdf)'''



                        '''if len(prop_before_transition_df) > 0:
                            print('prop_before_transition_df')
                            before_list.append(prop_before_transition_df)

                        else:
                            pass

                        if len(prop_after_transition_df) > 0:
                            print('prop_after_transition_df')
                            #todo have appended data here
                            after_start_date_gdf = prop_currency_test_df.append(prop_after_transition_df)
                            #todo sort data to latest TRAN DATE
                            final_gdf = filter_previous_data_latest_currency_date_fn(after_start_date_gdf)
                            after_list.append(final_gdf)

                        else:
                            pass'''

                        '''if len(prop_currency_transition_df.index) == len(prop_currency_test_df.index):
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
                            list_a.append(prop_currency_test_df)'''

                else:
                    pass

        print("Transition data exists  - check for other properties----------")
        print('transition_data_list_a - previous: ', transition_data_list_a)
        print(' - length: ', len(transition_data_list_a))
        print('prop_list - test:', prop_list)
        print(' - length: ', len(prop_list))
        print('-'*50)

        prop_to_keep_list = [x for x in transition_data_list_a if x not in prop_list]

        print('prop_to_keep_list: ', prop_to_keep_list)

        if len(prop_to_keep_list) > 0 and len(transition_data_list_a) != len(prop_list):
            print('keep > 0, tran != test')

            for prop_ in prop_to_keep_list:
                previous_other_gdf = original_transition_gdf[original_transition_gdf['PROPERTY'] == prop_]
                print(' - appending previous_other gdf to other list: ', prop_)
                other_list.append(previous_other_gdf)

        elif len(prop_to_keep_list) > 0 and len(transition_data_list_a) == len(prop_list):
            print('keep > 0, tran == test')
            for prop_ in prop_to_keep_list:
                previous_other_gdf = original_transition_gdf[original_transition_gdf['PROPERTY'] == prop_]
                print(' - appending previous_other gdf to other list: ', prop_)
                other_list.append(previous_other_gdf)

        else:
            print('NOT SURE IF ERROR -------- script not complete')
            print('- 1. same property, no other property required.')


        '''if len(prop_to_keep_list) > 0:
            print('other properties exist.')

            for prop_ in prop_to_keep_list:
                previous_other_gdf = original_transition_gdf[original_transition_gdf['PROPERTY'] == prop_]
                print(' - appending previous_other gdf to other list: ', prop_)
                other_list.append(previous_other_gdf)
        else:
            print('NO other properties exist')'''

        '''if len(prop_trans_to_keep_list) > 0:
            print('properties in transition do not occur in test.')

            for prop_ in prop_trans_to_keep_list:
                previous_other_gdf = original_transition_gdf[original_transition_gdf['PROPERTY'] == prop_]
                print(' - appending previous_other gdf to other list: ', prop_)
                other_list.append(previous_other_gdf)
        else:
            print('NO other properties exist')

        prop_test_to_keep_list = [x for x in prop_list if x not in transition_data_list_a]

        if len(prop_test_to_keep_list) > 0:
            print('properties in test do not occur in transition.')

            for prop_ in prop_trans_to_keep_list:
                previous_other_gdf = original_transition_gdf[original_transition_gdf['PROPERTY'] == prop_]
                print(' - appending previous_other gdf to other list: ', prop_)
                other_list.append(previous_other_gdf)
        else:
            print('NO other properties exist')'''

    else:

        # set variable_original_transition_gdf_status to False as NO transition data exists.
        original_transition_gdf_status = False
        print('No previous data exists.....')
        gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\1_5_no_previous_data_exists.csv")
        # gdf['DELETE'] = 0 # KEEP
        #gdf['NEW_DELETE'] = 0  # KEEP
        list_a.append(gdf)
        new_list.append(gdf)

    print('---------------- 1_5 lists ----------------')
    print('new: ', len(new_list))
    print('other: ', len(other_list))
    print('before: ', len(before_list))
    print('after: ', len(after_list))
    print('new_property: ', len(new_prop_list))
    print('---------------- END 1_5 ----------------')

    '''print('goodbye')
    import sys
    sys.exit()'''

    return new_list, other_list, before_list, after_list, new_prop_list, status_text


if __name__ == '__main__':
    main_routine()
