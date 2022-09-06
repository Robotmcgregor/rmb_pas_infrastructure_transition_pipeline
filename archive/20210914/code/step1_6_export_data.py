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


def compare_feature_feature_group_fn(input_list, compare_set, gdf, type_):
    """ Check that the input values match a loaded set. Also look for typos if ratio is greater than 80% swap the input
    value with the correct value. If no match or similar match can be made insert FAULTY.

    :param input_list: list object containing the variables from the dataframe to be checked against the compare set.
    :param compare_set: set object containing all of the accepted variables.
    """
    # print('='*50)
    # print('=' * 50)

    # print('compare_'*50)
    # print('input_list: ', input_list)

    status_list = []
    output_list = []
    for input_feature_group_ in input_list:
        n = 1
        # print('input_feature_group_: ', input_feature_group_)
        if type_ == 'property':
            input_feature_group = input_feature_group_.strip()

        else:
            input_feature_group = input_feature_group_.strip().title()

        no_match = "correct"
        if input_feature_group in compare_set:

            # print('=' * 50)
            output_list.append(input_feature_group)
            status_list.append('VERIFIED')
        else:

            print('Issue identified: ', input_feature_group)
            final_ratio_list = []
            for correct_feature_group in compare_set:

                # print('Checking against: ', correct_feature_group)
                ratio = levenshtein_ratio_and_distance(input_feature_group, correct_feature_group, ratio_calc=True)
                # print('levenshtein ratio (0.8 minimum): ', ratio)
                ratio_list = []
                if ratio >= 0.8:
                    # print('line 80: ', n)
                    while n == 1:
                        print(" - Did you mean.... '", correct_feature_group, "'?")
                        print(" -- I hope it's correct, cos that what I changed it to.")
                        print('-' * 50)
                        n += 1

                    ratio_list.append(correct_feature_group)
                    # gdf['STATUS'] = "feature auto updated"

                else:
                    ratio_list.append('Faulty')
                    # print('line 91: ', n)
                    while n == 1:
                        print('You entered: ', input_feature_group)
                        print(" - Seriously?? It's not even close...")
                        print(" -- I have changed the value to ERROR")
                        print('-' * 50)
                        n += 1
                    # gdf['STATUS'] = "feature unknown"
                    # n += 1
                # n +=1

                # print('ratio_list: ', ratio_list)
                final_ratio_list.extend(ratio_list)
            # print('final_ratio_list: ', final_ratio_list)
            final_ratio_list = [i for i in final_ratio_list if i != "Faulty"]
            # final_ratio_list.remove("Faulty")
            # print('l'*50)
            # print('final_ratio_list: ', final_ratio_list)

            if len(final_ratio_list) == 0:
                # print('add pass')
                output_list.append('FAULTY')
                status_list.append("ERROR")
            else:

                # print('add final_ratio_list')
                # print('final_ratio_list: ', final_ratio_list)
                output_list.extend(final_ratio_list)
                status_list.append("AUTO_CORRECT")

    # print('output_list: ', output_list)

    return output_list, gdf, status_list


def levenshtein_ratio_and_distance(s, t, ratio_calc=False):
    """ levenshtein_ratio_and_distance:
        Calculates levenshtein distance between two strings.
        If ratio_calc = True, the function computes the
        levenshtein distance ratio of similarity between two strings
        For all i and j, distance[i,j] will contain the Levenshtein
        distance between the first i characters of s and the
        first j characters of t
    """
    # Initialize matrix of zeros
    rows = len(s) + 1
    cols = len(t) + 1
    distance = np.zeros((rows, cols), dtype=int)

    # Populate matrix of zeros with the indices of each character of both strings
    for i in range(1, rows):
        for k in range(1, cols):
            distance[i][0] = i
            distance[0][k] = k

    # Iterate over the matrix to compute the cost of deletions,insertions and/or substitutions
    for col in range(1, cols):
        for row in range(1, rows):
            if s[row - 1] == t[col - 1]:
                cost = 0  # If the characters are the same in the two strings in a given position [i,j] then the cost is 0
            else:
                # In order to align the results with those of the Python Levenshtein package, if we choose to calculate the ratio
                # the cost of a substitution is 2. If we calculate just distance, then the cost of a substitution is 1.
                if ratio_calc == True:
                    cost = 2
                else:
                    cost = 1
            distance[row][col] = min(distance[row - 1][col] + 1,  # Cost of deletions
                                     distance[row][col - 1] + 1,  # Cost of insertions
                                     distance[row - 1][col - 1] + cost)  # Cost of substitutions
    if ratio_calc == True:
        # Computation of the Levenshtein Distance Ratio
        Ratio = ((len(s) + len(t)) - distance[row][col]) / (len(s) + len(t))
        return Ratio
    else:
        # print(distance) # Uncomment if you want to see the matrix showing how the algorithm computes the cost of deletions,
        # insertions and/or substitutions
        # This is the minimum number of edits needed to convert string a to string b
        return "The strings are {} edits away".format(distance[row][col])


def pastoral_estate_check(gdf, variable, variable2, dict_):
    for prop_ in gdf.PROPERTY.unique():

        property_filter = gdf[gdf['PROPERTY'] == prop_]
        prop_code = dict_[prop_]
        prop_tag_list = property_filter[variable].tolist()

        verified_tag_list = []
        prop_code_list = []
        for prop_tag in prop_tag_list:
            if prop_tag == prop_code:
                verified_tag_list.append('VERIFIED')
                prop_code_list.append(prop_code)
            else:
                verified_tag_list.append('AUTO-CORRECTED')

                print('Issue identified: ', prop_tag)
                print(' - I have changed it to ', prop_code, ".... You're welcome.")
                print('-' * 50)
                prop_code_list.append(prop_code)

        # gdf[variable] = prop_code_list
        # gdf[variable2] = verified_tag_list

    return gdf, verified_tag_list


def compare_dataframes_2_fn(previous, input_data_gdf):
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

    # --------------------------------------------------- left ---------------------------------------------------------
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_',
                                           on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                               'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'STATUS',
                                               'geometry'])

    input_left_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
    input_left_df['check'] = 'left'
    print('-' * 50)
    # print('input_left_df: ', list(input_left_df.columns))

    if 'merge' in input_left_df.columns:
        del input_left_df['merge_']
    if 'check' in input_left_df.columns:
        del input_left_df['check']
    if 'DATE_CURR_y' in input_left_df.columns:
        del input_left_df['DATE_CURR_y']

    # ---------------------------------------------------- right -------------------------------------------------------
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_',
                                           on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                               'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'STATUS',
                                               'geometry'])

    input_right_df = input_data_gdf2[input_data_gdf2['merge_'] == 'right_only']
    input_right_df['check'] = 'right'
    print('-' * 50)
    # print('input_right_df: ', list(input_right_df.columns))

    if 'merge' in input_right_df.columns:
        del input_right_df['merge_']
    if 'check' in input_right_df.columns:
        del input_right_df['check']
    if 'DATE_CURR_y' in input_right_df.columns:
        del input_right_df['DATE_CURR_y']

    # ---------------------------------------------- both -------------------------------------------------------

    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_',
                                           on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT',
                                               'PROPERTY',
                                               'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'STATUS',
                                               'geometry'])

    input_both_df = input_data_gdf2[input_data_gdf2['merge_'] == 'both']
    input_both_df['check'] = 'both'
    print('-' * 50)
    # print('input_both_df: ', list(input_both_df.columns))

    if 'merge' in input_both_df.columns:
        del input_both_df['merge_']
    if 'check' in input_both_df.columns:
        del input_both_df['check']
    if 'DATE_CURR_y' in input_both_df.columns:
        del input_both_df['DATE_CURR_y']

    # print('input_left_df: ', list(input_left_df.columns))
    print('input_left_df: ', input_left_df.shape)
    # print('input_right_df: ', list(input_right_df.columns))
    print('input_right_df: ', input_right_df.shape)
    # print('input_both_df: ', list(input_both_df.columns))
    print('input_both_df: ', input_both_df.shape)

    return input_left_df


def delete_observations_fn(previous, input_data_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print("delete_" * 20)
    print('previous data: ', previous.shape)
    print('data to be deleted: ', input_data_gdf.shape)

    if len(previous.index) < len(input_data_gdf.index):
        print('len(previous.index) < len(input_data_gdf.index)')
        # check for the same observations in the new data as in the previous data, and remove them from the new data.
        input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_')
        edited_data_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
        edited_data_df.drop(columns=['merge_'], inplace=True)
        print('Edited data df: ', edited_data_df.shape)

    elif len(previous.index) > len(input_data_gdf.index):
        print('len(previous.index) > len(input_data_gdf.index)')
        # check for the same observations in the new data as in the previous data, and remove them from the new data.
        input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_')
        edited_data_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
        edited_data_df.drop(columns=['merge_'], inplace=True)
        print('Edited data df: ', edited_data_df.shape)

    else:
        print('len(previous.index) == len(input_data_gdf.index)')
        edited_data_df = input_data_gdf

    return edited_data_df


'''def delete_observations_fn(previous, input_data_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print("delete_" * 20)
    print('previous data: ', previous.shape)
    print('data to be deleted: ', input_data_gdf.shape)

    if len(previous.index) < len(input_data_gdf.index):

        # check for the same observations in the new data as in the previous data, and remove them from the new data.
        input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_')
        edited_data_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
        edited_data_df.drop(columns=['merge_'], inplace=True)

    elif len(previous.index) > len(input_data_gdf.index):
        # check for the same observations in the new data as in the previous data, and remove them from the new data.
        input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_')
        edited_data_df = input_data_gdf2[input_data_gdf2['merge_'] == 'right_only']
        edited_data_df.drop(columns=['merge_'], inplace=True)

    else:
        edited_data_df = input_data_gdf

    print('Edited data df: ', edited_data_df.shape)
    return edited_data_df'''


def previous_data_check_fn(gdf, transition_dir, year, feature_item):
    print(':' * 50)
    print(' - Checking for previous ', feature_item, ' transition data....')
    print(':' * 50)

    list_a = []

    previous_dir_path = os.path.join(transition_dir, str(year), "previous_transfer", feature_item)
    print('previous_dir_path: ', previous_dir_path)
    previous_gdf_exists = False
    # migration_overwrite_list = []
    if glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

        for file in glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):
            print(' - located: ', file)
            # open as geo-dataframe
            previous_gdf = gpd.read_file(file, geometry="geometry")
            previous_gdf_exists = True
            # convert geo-dataframe to a pandas df
            previous_df = pd.DataFrame(previous_gdf)
            # convert string object to dateTime object
            previous_df['DATE_CURR_2'] = pd.to_datetime(previous_df['DATE_CURR'])
            print('.... located.')

            # convert geo-dataframe to a pandas df
            test_df = pd.DataFrame(gdf)
            # convert string object to dateTime object
            test_df['DATE_CURR_2'] = pd.to_datetime(test_df['DATE_CURR'])

            print('Filter the test and previous data:')
            # for loop through test dataframe based on property name.
            for prop_ in test_df.PROPERTY.unique():
                print(' - Property name: ', prop_)
                # filter dataframe based on property name.
                filter_test_df = test_df[test_df['PROPERTY'] == prop_]

                if len(filter_test_df.index) > 0:
                    print('- Number of observations: ', len(filter_test_df.index))
                    # for loop through filtered test dataframe based on currency date.
                    for currency in filter_test_df.DATE_CURR_2.unique():
                        print(' - Currency date: ', currency)
                        print('Checking for dates between:')
                        # create two variables start and end dates to use to filter previous data
                        start_date = currency + pd.Timedelta(days=15)
                        print(' - Start_date: ', start_date)
                        end_date = currency - pd.Timedelta(days=15)
                        print(' - End_date: ', end_date)

                        # filter the property filtered test data by currency date.
                        prop_curr_test_df = filter_test_df[filter_test_df['DATE_CURR_2'] == currency]
                        filter_previous_df = previous_gdf[
                            (previous_df['PROPERTY'] == prop_) & (previous_df['DATE_CURR_2'] < start_date) &
                            (previous_df['DATE_CURR_2'] > end_date)]
                        print('Filtered previous length: ', len(filter_previous_df.index))
                        print('=' * 50)
                        if len(filter_previous_df.index) > 0:
                            # determine the latest currency date.
                            date_list = []
                            for i in filter_previous_df.DATE_CURR_2.unique():
                                date_list.append(i)
                                # todo check that this actually works by adding fake data to the shapefile
                            print(' Checking for the latest property specific currency date of the previous data......')
                            latest_date = (max(date_list))
                            print(" - Latest_date: ", latest_date)

                            # filter the dataframe by the latest recorded curr_date
                            curr_date_previous_df = previous_gdf[previous_df['DATE_CURR_2'] == latest_date]

                            if len(curr_date_previous_df.index) > 0:
                                print(' - There are observations: ', len(filter_previous_df.index))
                                # Previous data exists
                                print('-' * 50)
                                print('Checking that the data is new.... line 512')
                                input_left_df, previous_left_df = compare_dataframes_fn(curr_date_previous_df,
                                                                                        prop_curr_test_df)

                                print('input_left_df: ', len(input_left_df.index), ' previous_left_df: ',
                                      len(previous_left_df.index))
                                print('input_left_df: ', input_left_df)
                                """print('The test data contains ', len(input_left_df.index),
                                      ' observations not in transition data.')
                                print('len(input_left_df.index): ', len(input_left_df.index))

                                print('The previous data contains: ', len(previous_left_df.index),
                                      ' observations not in test data.')
                                print('len(previous_left_df.index): ', len(previous_left_df.index))"""

                                if len(input_left_df.index) == 0 and len(previous_left_df.index) == 0:

                                    print(' - Data already exists in the transition directory. line 587')
                                    # final_gdf = None
                                    """prop_curr_test_df['DELETE'] = 3
                                    add_to_previous_list.append(prop_curr_test_df)
                                    curr_date_previous_df['DELETE'] = 3
                                    delete_from_previous_list.append(curr_date_previous_df)"""
                                    print('-' * 50)
                                    # prop_curr_test_df['DELETE'] = 2
                                    list_a.append(prop_curr_test_df)
                                    # print('gdf 505: ', prop_curr_test_df)
                                    # final_previous_transition_list.append(None)
                                    # print('-' * 50)

                                elif len(input_left_df.index) > len(previous_left_df.index):
                                    print('The TEST data contains ',
                                          str(len(input_left_df.index) - len(previous_left_df.index)),
                                          ' more observations than the PREVIOUS data.')
                                    print('len(input_left_df.index): ', len(input_left_df.index))
                                    print('len(previous_left_df.index): ', len(previous_left_df.index))
                                    # delete_list.append(curr_date_previous_df)
                                    # delete_prev_list.append(prop_curr_test_df)
                                    """final_previous_transition_list.append(prop_curr_test_df)
                                    prop_curr_test_df['DELETE'] = 0
                                    add_to_previous_list.append(prop_curr_test_df)
                                    curr_date_previous_df['DELETE'] = 1"""
                                    """delete_from_previous_list.append(curr_date_previous_df)"""
                                    # prop_curr_test_df['DELETE'] = 0
                                    list_a.append(prop_curr_test_df)
                                    # print('keep_gdf 524: ', prop_curr_test_df)
                                    # curr_date_previous_df['DELETE'] = 1
                                    # list_a.append(curr_date_previous_df)
                                    # print('delete_gdf 527: ', curr_date_previous_df)
                                    print('-' * 50)

                                elif len(input_left_df.index) < len(previous_left_df.index):
                                    print('The PREVIOUS data contains ',
                                          str(len(previous_left_df.index) - len(input_left_df.index)),
                                          ' more observations than the TEST data.')
                                    print('len(input_left_df.index): ', len(input_left_df.index))
                                    print('len(previous_left_df.index): ', len(previous_left_df.index))
                                    # delete_list.append(curr_date_previous_df)
                                    # delete_prev_list.append(prop_curr_test_df)
                                    # final_previous_transition_list.append(prop_curr_test_df)
                                    """curr_date_previous_df['DELETE'] = 0
                                    add_to_previous_list.append(curr_date_previous_df)
                                    prop_curr_test_df['DELETE'] = 1
                                    delete_from_previous_list.append(prop_curr_test_df)"""

                                    # curr_date_previous_df['DELETE'] = 0
                                    # list_a.append(curr_date_previous_df)
                                    # print('keep_gdf 546: ', curr_date_previous_df)
                                    # prop_curr_test_df['DELETE'] = 1
                                    list_a.append(prop_curr_test_df)
                                    # print('delete_gdf 549: ', prop_curr_test_df)

                                    print('-' * 50)

                            else:
                                print('line 584 - else')
                                # prop_curr_test_df['DELETE'] = 0
                                list_a.append(prop_curr_test_df)
                                print('add_gdf 556: ', prop_curr_test_df)

                        else:
                            # prop_curr_test_df['DELETE'] = 0
                            list_a.append(prop_curr_test_df)
                            print('add_gdf 561: ', prop_curr_test_df)
    else:
        print('No previous transition data exists.....')
        # final_previous_transition_list.append(gdf)
        # gdf['DELETE'] = 0
        list_a.append(gdf)

    return list_a, previous_gdf_exists


def transition_export_gdf_fn(gdf, transition_dir, feature_type, year):
    """ Export final shapefile and update the currency date field.

    :param transition_dir:
    :param gdf:
    :param feature_type:
    """

    year_dir = ("{0}\\{1}".format(transition_dir, year))
    check_dir = os.path.isdir(year_dir)
    if not check_dir:
        os.mkdir(year_dir)

    type_dir = ("{0}\\{1}".format(year_dir, feature_type))
    check_dir = os.path.isdir(type_dir)
    if not check_dir:
        os.mkdir(type_dir)

    shapefile = ("{0}\\{1}\\{2}\\{3}_gda94.shp".format(transition_dir, year, feature_type, feature_type))

    check_file = os.path.exists(shapefile)

    if check_file:
        existing_data_gdf = gpd.read_file(shapefile)
        existing_data_gdf.append(gdf)
        print(' - Appended to existing transition data.')

    else:
        print(' - Uploaded to transition directory.')
        gdf.to_file(shapefile, driver="ESRI Shapefile")


'''def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, dir_list_item, year):
    print("property_export_shapefile_fn")
    for prop_ in prop_curr_test.PROPERTY.unique():
        print(prop_)
        prop_filter = prop_curr_test[prop_curr_test['PROPERTY'] == prop_]
        dist_ = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DISTRICT'].iloc[0]
        prop_code = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'PROP_TAG'].iloc[0]
        currency = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DATE_CURR'].iloc[0]

        property_name = "{0}_{1}".format(prop_code, prop_.replace(' ', '_').title())
        dist = dist_.replace(' ', '_')

        if dist == 'Northern_Alice_Springs':
            district = 'Northern_Alice'
        elif dist == 'Southern_Alice_Springs':
            district = 'Southern_Alice'
        elif dist == 'Victoria_River':
            district = 'VRD'
        else:
            district = dist

        datetime_object = datetime.strptime(currency, "%Y-%m-%d").date()
        date_str = datetime_object.strftime("%Y%m%d")

        # now = datetime.now()
        # date_str = now.strftime("%Y%m%d")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), dir_list_item)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter['UPLOAD'] = 'Transition'
        output = ("{0}\\{1}_transition_{2}.shp".format(output_path, dir_list_item, date_str))
        print(' -- ', output)
        print('+'*50)
        print('+' * 50)
        print('+' * 50)
        print(prop_filter.crs)
        print('+' * 50)
        print('+' * 50)
        prop_filter.to_file(output, driver="ESRI Shapefile")

        prop_filter.to_csv("{0}\\{1}_transition_{2}.csv".format(output_path, dir_list_item, date_str))
        print(' - output shapefile to: ', "{0}\\{1}_transition_{2}.csv".format(output_path, dir_list_item, date_str))'''


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


def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, dir_list_item, year):
    print("property_export_shapefile_fn")
    for prop_ in prop_curr_test.PROPERTY.unique():
        print(prop_)
        prop_filter = prop_curr_test[prop_curr_test['PROPERTY'] == prop_]
        dist_ = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DISTRICT'].iloc[0]
        prop_code = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'PROP_TAG'].iloc[0]
        currency = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DATE_CURR'].iloc[0]

        property_name = "{0}_{1}".format(prop_code, prop_.replace(' ', '_').title())
        dist = dist_.replace(' ', '_')

        if dist == 'Northern_Alice_Springs':
            district = 'Northern_Alice'
        elif dist == 'Southern_Alice_Springs':
            district = 'Southern_Alice'
        elif dist == 'Victoria_River':
            district = 'VRD'
        else:
            district = dist

        datetime_object = datetime.strptime(currency, "%Y-%m-%d").date()
        date_str = datetime_object.strftime("%Y%m%d")

        # now = datetime.now()
        # date_str = now.strftime("%Y%m%d")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), dir_list_item)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter['UPLOAD'] = 'Transition'
        output = ("{0}\\{1}_transition_{2}.shp".format(output_path, dir_list_item, date_str))
        print(' -- ', output)
        print(' -- ', output)
        print('+'*50)
        print('+' * 50)
        print('+' * 50)
        print(prop_filter.crs)
        #prop_filter.set_crs('epsg:4283')
        print(prop_filter.crs)
        print('+' * 50)
        print('+' * 50)
        prop_filter.to_file(output, driver="ESRI Shapefile")

        prop_filter.to_csv("{0}\\{1}_transition_{2}.csv".format(output_path, dir_list_item, date_str))
        print(' - output shapefile to: ', "{0}\\{1}_transition_{2}.csv".format(output_path, dir_list_item, date_str))


def convert_to_geo_df_remove_date_curr_2_fn(df):
    # convert concatenated df to geo-dataframe
    final_gdf = gpd.GeoDataFrame(df, geometry='geometry')

    # delete DATE_CURR_2 from column headers
    if 'DATE_CURR_2' in final_gdf.columns:
        del final_gdf['DATE_CURR_2']

    return final_gdf


def compare_previous_dataframes_fn(previous_df, prop_filter_previous):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print('previous data: ', previous_df.shape)
    print('input_data_gdf: ', prop_filter_previous.shape)
    print('columns_' * 20)
    print('previous data: ', list(previous_df.columns))
    print('input_data_gdf: ', list(prop_filter_previous.columns))

    previous_df.drop(columns=['DATE_CURR_2'], inplace=True)
    prop_filter_previous.drop(columns=['DATE_CURR_2'], inplace=True)

    print('previous: ', previous_df)
    print('prop_filter_previous: ', prop_filter_previous)

    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    # previous.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\previous_line274.csv")
    # input_data_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_data_line275.csv")
    prop_filter_previous2 = prop_filter_previous.merge(previous_df, how='outer', indicator='merge_',
                                                       on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT',
                                                           'PROPERTY',
                                                           'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE',
                                                           'STATUS',
                                                           'geometry'])

    print('prop_filter_previous2: ', prop_filter_previous2)
    input_left_df = prop_filter_previous2[prop_filter_previous2['merge_'] == 'left_only']
    input_left_df['check'] = 'left'

    print('-' * 50)
    print(len(input_left_df.index))

    # edited_data_df.drop(columns=['merge_'], inplace=True)
    # input_left_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_left_df_292.csv")
    # ------------------------------------------------------------------------------------------------------------------
    """previous_left_df2 = previous.merge(input_data_gdf, how='outer', indicator='merge_',
                                       on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                           'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE', 'STATUS',
                                           'geometry'])"""

    return input_left_df


def compare_dataframes_fn(transition_gdf, delete_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print('previous data: ', transition_gdf.shape)
    print('input_data_gdf: ', delete_gdf.shape)
    print('columns_' * 20)
    print('previous data: ', list(transition_gdf.columns))
    print('input_data_gdf: ', list(delete_gdf.columns))

    # previous.drop(columns=['DATE_CURR_2'], inplace=True)
    # input_data_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    transition_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\transition_gdf.csv")
    delete_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\delete_gdf.csv")
    merged_gdf = delete_gdf.merge(transition_gdf, how='outer', indicator='merge_',
                                  on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                      'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE', 'STATUS',
                                      'geometry'])
    merged_right_gdf = merged_gdf[merged_gdf['merge_'] == 'right_only']
    merged_right_gdf['compare'] = 'right'
    print('-' * 50)

    merged_right_gdf.drop(columns=['merge_'], inplace=True)
    merged_right_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\merged_right.csv")

    print('merged_right_gdf: ', merged_right_gdf)
    return merged_right_gdf


def delete_and_export_fn(final_gdf, previous_dir_path, feature_type):

    if "DATE_CURR_2" in final_gdf.columns:
        final_gdf.drop(columns=["DATE_CURR_2"], inplace=True)

    print('previous_dir_path: ', previous_dir_path)
    for files in glob("{0}\\{1}".format(previous_dir_path, "*")):
        print(' - removed: ', files)
        # open as geo-dataframe

        #todo check if I need to remove or just overwrite shapefile
        #os.remove(files)

    print('----------- DROP DUPLICATES ---------------')
    final_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\before_drop_duplicates.csv")
    final_gdf.drop_duplicates(inplace=True)
    final_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\after_drop_duplicates.csv")

    final_gdf.to_file(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\final_gdf.shp',
                      driver="ESRI Shapefile")
    #print(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\final_gdf.shp')

    final_gdf.to_file("{0}\\{1}{2}{3}".format(previous_dir_path, "previous_transfer_", feature_type, "_gda94.shp"),
                      driver="ESRI Shapefile")


def for_migration_export_shapefile_fn(keep_gdf, for_migration_dir_path, feature_type):

    date_time = datetime.now()
    str_date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
    keep_gdf['M_DATE']= str_date_time
    output = "{0}\\Pastoral_Infra_{1}.shp".format(for_migration_dir_path, feature_type.title())
    print('output_output-output: ', output)
    keep_gdf.to_file(output, driver="ESRI Shapefile")


def main_routine(final_test_gdf, final_test_gdf_status, status_text, other_properties_gdf,
                 other_properties_output_status, prop_start_end_dates_gdf, prop_start_end_dates_output_status,
                 transition_dir, year, assets_dir, pastoral_estate, pastoral_districts_path, feature_type,
                 original_transition_gdf):
    """

    """
    print('1_6__' * 50)
    print('step1_6 initiated')
    print('working on: ', feature_type)
    print('1_6__' * 50)

    transition_data_list_a = final_test_gdf.PROPERTY.unique().tolist()
    print(';' * 20)
    print('step1_6 prop names in final_test_gdf: ', transition_data_list_a)

    previous_dir_path = os.path.join(transition_dir, str(year), "previous_transfer", feature_type)
    print('previous_dir_path: ', previous_dir_path)

    for_migration_dir_path = os.path.join(transition_dir, str(year), "for_migration", feature_type)
    print('for_migration_dir_path: ', for_migration_dir_path)
    
    print(' --------------------- file_output workflow --------------------------')
    if final_test_gdf_status:
        print('L'*50)
        print('1. Data for export is present')

        #print('final_test_gdf_status: ', final_test_gdf_status)
        #print('L' * 50)

        # create a keep and delete geo-dataframe based on a copy of the final_test_gdf
        final_test_gdf2 = final_test_gdf.copy()
        final_test_gdf.to_csv(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\final_test_data.csv')
        if 'NEW_DELETE' in final_test_gdf:
            keep_gdf = final_test_gdf2[final_test_gdf2['NEW_DELETE'] == 0]
            del keep_gdf['NEW_DELETE']
        else:
            keep_gdf = final_test_gdf

        print('keep_gdf length: ', len(keep_gdf.index))


        if len(keep_gdf.index) > 0:
            print('2. Keep_gdf has observations')
            # export to the property specific data_upload directory
            property_export_shapefile_fn(keep_gdf, pastoral_districts_path, feature_type, year)

            # export data to the FOR MIGRATION  directory
            for_migration_export_shapefile_fn(keep_gdf, for_migration_dir_path, feature_type)

            if other_properties_output_status:
                print('-'*50)
                print('3. Other properties have been identified')

                transition_data_list_a = other_properties_gdf.PROPERTY.unique().tolist()
                print(';' * 20)
                print('step1_6 prop names in other_properties_gdf: ', transition_data_list_a)

                print('other_properties_gdf: ', other_properties_gdf)


                other_and_keep_gdf = keep_gdf.append(other_properties_gdf)
                print('4. keep gdf and other properties have been appended.')
                other_and_keep_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\other_and_keep_gdf.csv")


                if prop_start_end_dates_output_status:
                    print('5. Properties stat and end dates have been identified.')
                    print('prop_start_end_dates_gdf: ', prop_start_end_dates_gdf)
                    prop_start_end_dates_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\prop_start_end_dates_gdf.csv")
                    prop_se_other_and_keep_gdf = other_and_keep_gdf.append(prop_start_end_dates_gdf)
                    print('Call the delete and export function...... prop_se_other_and_keep_gdf')
                    # purge the directory and output new transition data
                    delete_and_export_fn(prop_se_other_and_keep_gdf, previous_dir_path, feature_type)

                else:
                    print('5. Properties stat and end dates have ----- NOT ---- been identified.')

                    print('prop_start_end_dates_output_status: ', prop_start_end_dates_output_status)
                    # prop_se_other_and_keep_gdf = other_and_keep_gdf
                    print('Call the delete and export function...... other_and_keep_gdf')
                    # purge the directory and output new transition data

                    other_and_keep_gdf.to_csv(
                        r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\other_properties_gdf.csv')
                    transition_data_list_a = other_and_keep_gdf.PROPERTY.unique().tolist()
                    print(';' * 20)
                    print('step1_6 prop names in other_and_keep_gdf: ', transition_data_list_a)
                    delete_and_export_fn(other_and_keep_gdf, previous_dir_path, feature_type)

            else:

                print('3. Other properties have ---- NOT ---- been identified')
                #print('other_properties_output_status: ', other_properties_output_status)
                print('Call the delete and export function...... keep_gdf')
                # purge the directory and output new transition data
                delete_and_export_fn(keep_gdf, previous_dir_path, feature_type)


        else:
            print("2. keep gdf has no data NO rules set yet")
    else:

        print('L'*50)
        print('1. New data has ------ NOT ---- been identified no rules set yet')
        print('L' * 50)


        if glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):

            for file in glob("{0}\\{1}".format(previous_dir_path, "previous_transfer_*_gda94.shp")):
                print(' - located: ', file)
                # open as geo-dataframe
                prop_se_other_and_keep_gdf = gpd.read_file(file, geometry="geometry")

        else:
            # No new data and no transition data.
            print('No new data and no transition data.')


    return status_text


if __name__ == '__main__':
    main_routine()
