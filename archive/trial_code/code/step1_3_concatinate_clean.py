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
    #print('='*50)
    #print(len(gdf.index))
    #print('=' * 50)

    # print('compare_'*50)
    # print('input_list: ', input_list)

    status_list = []
    output_list = []
    for input_feature_group_ in input_list:
        n = 1
        # print('input_feature_group_: ', input_feature_group_)
        if type_ == 'property':
            #print('type = property..')
            input_feature_group = input_feature_group_.strip()

        else:
            #print('type does != property')
            input_feature_group = input_feature_group_.strip().title()

        no_match = "correct"
        if input_feature_group in compare_set:

            #print('=' * 50)
            #print('input_feature_group was in: ', compare_set)
            output_list.append(input_feature_group)
            status_list.append('VERIFIED')
            #print('verified')
        else:

            #print('Issue identified line 75: ', input_feature_group)
            final_ratio_list = []
            for correct_feature_group in compare_set:


                # print('Checking against: ', correct_feature_group)
                ratio = levenshtein_ratio_and_distance(input_feature_group, correct_feature_group, ratio_calc=True)
                # print('levenshtein ratio (0.8 minimum): ', ratio)
                ratio_list = []
                if ratio >= 0.8:
                    print('line 85: ', n)
                    while n == 1:
                        print(" - Did you mean.... '", correct_feature_group, "'?")
                        print(" -- I hope it's correct, cos that what I changed it to.")
                        print('-' * 50)
                        n += 1

                    ratio_list.append(correct_feature_group)
                    #print("correct_feature_group: ", correct_feature_group)

                    #status_list.append("AUTO_CORRECT")
                    # gdf['STATUS'] = "feature auto updated"

                else:
                    ratio_list.append('Faulty')
                    #print('line 100: ', n)
                    while n == 1:
                        print('You entered: ', input_feature_group)
                        print(" - Seriously?? It's not even close...")
                        print(" -- I have changed the value to ERROR")
                        print('-' * 50)
                        n += 1
                    # gdf['STATUS'] = "feature unknown"
                    # n += 1
                # n +=1

                #print('ratio_list 111: ', ratio_list)
                final_ratio_list.extend(ratio_list)
            #print('final_ratio_list 113: ', final_ratio_list)
            final_ratio_list = [i for i in final_ratio_list if i != "Faulty"]
            # final_ratio_list.remove("Faulty")
            #print('l'*50)
            #print('final_ratio_list: ', final_ratio_list)

            if len(final_ratio_list) == 0:
                #print('add pass line 120')
                output_list.append('FAULTY')
                status_list.append("ERROR")
            else:

                # print('add final_ratio_list')
                #print('}'*50)
                #print('final_ratio_list 127: ', final_ratio_list)
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


def concat_list_to_df_fn(list_a):
    """ Define the method used to concatenate the data to a dataframe based on the length of the list.

    :param list_a: list object or a list of list object.
    :return df_concat: pandas dataframe object crated from the input list or list of lists (list_a):
    """
    if len(list_a) <= 1:
        df_concat = pd.DataFrame(list_a[0]).transpose()

    else:
        # df_concat = pd.concat(list_a)
        df_concat = pd.DataFrame.from_records(list_a)

    return df_concat


def identify_set_fn(feature_type):
    """ Determine the feature group and feature set based on the feature type.

    :param feature_type: string object containing the type of feature (point, line polygon)
    :return feature_group_set: set object containing the relevant feature group categories
    :return feature_set: set object containing the relevant feature categories
    """
    if feature_type == 'points':
        feature_group_set = {'Aviation Points', 'Transport Points', 'Water Points', 'Cultural Points',
                             'Hydro Points', 'Building Points'}
        feature_set = {'Stock Yard', 'Waterhole', 'Aerial or Tower', 'Turkey Nest', 'Trough', 'Bore',
                       'Water Tank',
                       'Homestead', 'Landing Ground', 'General Cultural Feature', 'Spring', 'Dam',
                       'Underground Mine', 'General Building', 'Pump Out Point', 'Gate'}

    elif feature_type == 'lines':
        feature_group_set = {'Water Lines', 'Transport Lines', 'Cultural Lines'}
        feature_set = {'Vehicle Track', 'Cleared Line', 'Water Pipeline', 'Fenceline'}

        '''elif feature_type == 'paddocks':
            feature_group_set = {'Cultural Areas'}
            feature_set = {'Paddock'}'''

    elif feature_type == 'polygons':
        print('feature_type: ', feature_type)

        feature_group_set = {'Aviation Areas', 'Cultural Areas'}
        feature_set = {'Quarry', 'General Cultural Feature', 'Landing Strip', 'Paddock'}

    else:
        pass

    return feature_group_set, feature_set


def pastoral_estate_check(gdf, variable_, variable2, dict_):

    print(' variable: ', variable_)
    print("variable2: ", variable2)
    final_verified_tag_list = []
    final_prop_code_list = []

    for prop_ in gdf.PROPERTY.unique():
        print('pastoral_estate_check: ', prop_)
        print('gdf has been filtered by: ', prop_)
        property_filter = gdf[gdf['PROPERTY'] == prop_]
        print(' - using: ', dict_)
        prop_code = dict_[prop_]
        print(' - I have extracted this: ', prop_code)
        prop_tag_list = property_filter[variable_].tolist()
        print('-- Using column heading: ', variable_)
        print(' -- I have created this list: ', prop_tag_list)

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

        final_verified_tag_list.extend(verified_tag_list)
        final_prop_code_list.extend(prop_code_list)
        # gdf[variable] = prop_code_list
        # gdf[variable2] = verified_tag_list

    gdf[variable_] = final_prop_code_list

    return gdf, final_verified_tag_list


def create_property_name_set(pastoral_estate):
    pastoral_estate_gdf = gpd.read_file(pastoral_estate)
    property_name_list = pastoral_estate_gdf.PROPERTY.tolist()
    property_name_set = set(property_name_list)

    return pastoral_estate_gdf, property_name_set


def check_property_details(pastoral_estate_gdf, gdf):
    prop_dist_dict = pastoral_estate_gdf.set_index('PROPERTY').to_dict()['DISTRICT']
    print('prop_dist_dict: ', prop_dist_dict)
    prop_prop_code_dict = pastoral_estate_gdf.set_index('PROPERTY').to_dict()['PROP_TAG']
    print('prop_prop_code_dict: ',prop_prop_code_dict)
    property_name_list = pastoral_estate_gdf.PROPERTY.tolist()
    property_name_set = set(property_name_list)
    gdf2, district_verified_list = pastoral_estate_check(gdf, 'DISTRICT', 'DISTRICT_CHECK', prop_dist_dict)
    gdf3, prop_tag_verified_list = pastoral_estate_check(gdf2, 'PROP_TAG', 'PROP_TAG_CHECK', prop_prop_code_dict)

    return gdf3, property_name_set, district_verified_list, prop_tag_verified_list


def compare_dataframes_fn(previous, input_data_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    #print('previous data: ', previous.shape)
    #print('input_data_gdf: ', input_data_gdf.shape)
    #print('columns_'*20)
    #print('previous data: ', list(previous.columns))
    #print('input_data_gdf: ', list(input_data_gdf.columns))

    previous.drop(columns=['DATE_CURR_2'], inplace=True)
    input_data_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    previous.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\previous_line274.csv")
    input_data_gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_data_line275.csv")
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_',
                                           on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                               'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE', 'STATUS',
                                               'geometry'])
    input_left_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
    input_left_df['check'] = 'left'
    print('-' * 50)

    # edited_data_df.drop(columns=['merge_'], inplace=True)
    input_left_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\input_left_df_292.csv")
    # ------------------------------------------------------------------------------------------------------------------
    previous_left_df2 = previous.merge(input_data_gdf, how='outer', indicator='merge_',
                                       on=['FEATGROUP', 'FEATURE', 'LABEL', 'DATE_INSP', 'DISTRICT', 'PROPERTY',
                                           'PROP_TAG', 'SOURCE', 'CONFIDENCE', 'MAPDISPLAY', 'DELETE', 'STATUS',
                                           'geometry'])

    previous_left_df = previous_left_df2[previous_left_df2['merge_'] == 'left_only']
    previous_left_df['check'] = 'left'
    print('-' * 50)
    # edited_data_df.drop(columns=['merge_'], inplace=True)
    previous_left_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\previous_left_df2_line_300.csv")

    return input_left_df, previous_left_df



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
        prop_filter.to_file(output, driver="ESRI Shapefile")

        prop_filter.to_csv("{0}\\{1}_transition_{2}.csv".format(output_path, dir_list_item, date_str))


def calculate_area_km2_fn(gdf):
    """ Calculate the area in Australian Albers.

    """
    gdf2 = gdf.copy()
    gdf3 = gdf2.to_crs(epsg=3577)
    gdf3["AREA"] = gdf3["geometry"].area / 10 ** 6
    gdf4 = gdf3.to_crs(epsg=4283)

    return gdf4


def calculate_length_km2_fn(gdf):
    """ Calculate the area in Australian Albers.

    """
    gdf2 = gdf.copy()
    gdf3 = gdf2.to_crs(epsg=3577)
    gdf3["LENGTH__"] = gdf3["geometry"].length / 10 ** 6
    gdf4 = gdf3.to_crs(epsg=4283)

    return gdf4


def previous_update_folder_structure(dir_list_item, transition_dir, year):
    year_dir = (
        '{0}\\{1}'.format(transition_dir, year))
    print('year_dir: ', year_dir)
    check_dir = os.path.isdir(year_dir)
    if not check_dir:
        os.mkdir(year_dir)

    previous_dir = "{0}\\previous_transfer".format(year_dir, dir_list_item)
    print('previous_dir created: ', previous_dir)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(previous_dir)

    dir_list_dir = "{0}\\{1}".format(previous_dir, dir_list_item)
    print(':'*50)
    print('previous dir_list_dir: ', dir_list_dir)
    print('dir_list_dir: ', dir_list_dir)
    check_dir = os.path.isdir(dir_list_dir)
    if not check_dir:
        os.mkdir(dir_list_dir)


def previous_transfer_export_gdf_fn(gdf, transition_dir, feature_type, year):
    """ Export previous shapefile or append existing shapefile.

    :param year:
    :param transition_dir:
    :param gdf:
    :param feature_type:
    """

    shapefile = (
        "{0}\\{1}\\previous_transfer\\{2}\\previous_transfer_{3}_gda94.shp".format(transition_dir, year, feature_type,
                                                                                   feature_type))

    check_file = os.path.exists(shapefile)

    if check_file:
        existing_data_gdf = gpd.read_file(shapefile)
        existing_data_gdf.append(gdf)
        print(' - data appended: ', shapefile)

    else:
        print(' - data transferred: ', shapefile)
        gdf.to_file(shapefile, driver="ESRI Shapefile")

    return gdf


def main_routine(gdf, feature_type, transition_dir, year, status_text, assets_dir, pastoral_estate,
                 pastoral_districts_path):
    """

    """
    print('='*50)
    print('step1_3 initiated')
    print('working on: ', feature_type)
    print('=' * 50)

    pastoral_estate_gdf, property_name_set = create_property_name_set(pastoral_estate)
    print('-' * 50)
    print('Let me check your feature attributes for obvious errors..........')
    # process the feature
    feature_list = gdf.PROPERTY.tolist()
    # check if variables in column are correct repair if possible
    print('line 865')
    output_list, gdf, p_status_list = compare_feature_feature_group_fn(feature_list, property_name_set, gdf, 'property')
    gdf['PROPERTY'] = output_list
    print('p_status_list: ', p_status_list)
    #gdf['P_STATUS'] = p_status_list

    # call the identity_set_fn to determine the feature group and feature sets relevant to the feature type.
    #print('feature type: ', feature_type)
    #print('=' * 25)
    feature_group_set, feature_set = identify_set_fn(feature_type)

    # process the feature group
    feature_list = gdf.FEATGROUP.tolist()
    feature_group_unique = gdf.FEATGROUP.unique().tolist()
    print('feature_group_unique: ', feature_group_unique)
    # check if variables in column are correct repair if possible
    #print('l'*50)
    #print(feature_list)
    #print(feature_group_set)
    print('line 881')
    output_list, gdf, fg_status_list = compare_feature_feature_group_fn(feature_list, feature_group_set, gdf,
                                                                        'feature_group')
    gdf['FEATGROUP'] = output_list
    print('fg_status_list: ', fg_status_list)
    #gdf['FG_STATUS'] = fg_status_list

    # process the feature
    feature_list = gdf.FEATURE.tolist()
    # check if variables in column are correct repair if possible
    print('line 890')
    output_list, gdf, f_status_list = compare_feature_feature_group_fn(feature_list, feature_set, gdf, 'feature')
    print('f_status_list: ',  f_status_list)

    gdf['FEATURE'] = output_list
    #gdf['F_STATUS'] = f_status_list

    gdf, property_name_set, district_verified_list, prop_tag_verified_list = check_property_details(pastoral_estate_gdf,
                                                                                                    gdf)

    print("district_verified_list: ", district_verified_list)
    print("prop_tag_verified_list", prop_tag_verified_list)
    print('completed: check_property_details')
    # overwrite status list with comments for property, district, prop_tag, feature and feature group.
    result_list1 = [y if x == "VERIFIED" else x for x, y in zip(fg_status_list, f_status_list)]
    print('len results 1: ', len(result_list1))
    result_list2 = [y if x == "VERIFIED" else x for x, y in zip(result_list1, p_status_list)]
    print('len results 2: ', len(result_list2))
    result_list3 = [y if x == "VERIFIED" else x for x, y in zip(result_list2, district_verified_list)]
    print('len results 3: ', len(result_list3))
    result_list = [y if x == "VERIFIED" else x for x, y in zip(result_list3, prop_tag_verified_list)]
    print('len results: ', len(result_list))

    print(' Status column has been updated to reflect any changes or errors identified.')
    print('result_list: ', result_list)

    print("length of results list: ", len(result_list))
    print("length of gdf: ", len(gdf.index))
    gdf["STATUS"] = result_list

    return status_text, gdf



if __name__ == '__main__':
    main_routine()
