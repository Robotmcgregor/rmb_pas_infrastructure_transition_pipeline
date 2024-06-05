# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_3_concatenate_clean.py
--------------------------------------

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
import warnings
import pandas as pd
import geopandas as gpd
import numpy as np
warnings.filterwarnings("ignore")


def compare_features_against_data_sets_fn(input_list, compare_set, gdf, type_):
    """ Check that the input values match a loaded set. Also look for typos if ratio is greater than 80% swaps the input
    value with the correct value. If no similar match can be made insert FAULTY.

    :param type_: string object containing feature type to navigate workflow.
    :param gdf: geo-dataframe object containing data to be tested.
    :param input_list: list object containing the variables from the dataframe to be checked against the compare set.
    :param compare_set: set object containing all of the accepted variables.
    """

    status_list = []
    output_list = []
    for input_feature_group_ in input_list:
        n = 1
        # print('input_feature_group_: ', input_feature_group_)
        if type_ == 'property':
            input_feature_group = input_feature_group_.strip()

        else:
            # print('type does != property')
            input_feature_group = input_feature_group_.strip().title()

        no_match = "correct"
        if input_feature_group in compare_set:

            output_list.append(input_feature_group)
            status_list.append('VERIFIED')
        else:

            final_ratio_list = []
            for correct_feature_group in compare_set:

                ratio = levenshtein_ratio_and_distance(input_feature_group, correct_feature_group, ratio_calc=True)
                # set levenshtein ratio to a minimum of 80%.
                ratio_list = []
                if ratio >= 0.8:
                    # print('line 85: ', n)
                    while n == 1:
                        print(" - Did you mean.... '", correct_feature_group, "'?")
                        print(" -- I hope it's correct, cos that what I changed it to.")
                        print('-' * 50)
                        n += 1
                    ratio_list.append(correct_feature_group)

                else:
                    ratio_list.append('Faulty')
                    # print('line 100: ', n)
                    while n == 1:
                        print('You entered: ', input_feature_group)
                        print(" - Seriously?? It's not even close...")
                        print(" -- I have changed the value to ERROR")
                        print('-' * 50)
                        n += 1

                final_ratio_list.extend(ratio_list)
            # remove faulty form list.
            final_ratio_list = [i for i in final_ratio_list if i != "Faulty"]

            if len(final_ratio_list) == 0:
                output_list.append('FAULTY')
                status_list.append("ERROR")
            else:

                output_list.extend(final_ratio_list)
                status_list.append("AUTO_CORRECT")

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
                cost = 0
                # If the characters are the same in the two strings in a given position [i,j] then the cost is 0
            else:
                # In order to align the results with those of the Python Levenshtein package, if we choose to calculate
                # the ratio
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
        ratio = ((len(s) + len(t)) - distance[row][col]) / (len(s) + len(t))
        return ratio
    else:

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

    :param feature_type: string object containing the type of feature (point, line polygon).
    :return feature_group_set: set object containing the relevant feature group categories.
    :return feature_set: set object containing the relevant feature categories.
    """
    if feature_type == 'points':
        feature_group_set = {'Aviation Points', 'Transport Points', 'Water Points', 'Cultural Points',
                             'Hydro Points', 'Building Points', 'Monitoring Sites'}
        feature_set = {'Stock Yard', 'Waterhole', 'Aerial Or Tower', 'Turkey Nest', 'Trough', 'Bore', 'Water Tank',
                       'Homestead', 'Landing Ground', 'General Cultural Feature', 'Spring', 'Dam', 'Underground Mine',
                       'General Building', 'Pump Out Point', 'Gate', 'Integrated', 'Integrated Centre', 'Tier 1',
                       'Quarry', 'Other'}

    elif feature_type == 'lines':
        feature_group_set = {'Water Lines', 'Transport Lines', 'Cultural Lines'}
        feature_set = {'Vehicle Track', 'Vehicle Track (Abd.)', 'Cleared Line', 'Water Pipeline',
                       'Fenceline', 'Fenceline (Abd.)', 'Fence Open', 'Fence Neighbour', }

    elif feature_type == 'paddocks':
            feature_group_set = {'Cultural Areas'}
            feature_set = {'Paddock', 'Paddock Open'}

    elif feature_type == 'polygons':
        print('feature_type: ', feature_type)

        feature_group_set = {'Aviation Areas', 'Cultural Areas'}
        feature_set = {'Quarry', 'General Cultural Feature', 'Landing Strip'}

    else:
        pass

    return feature_group_set, feature_set


def pastoral_estate_check_fn(gdf, variable_, dict_):
    """

    :param gdf: geo-dataframe object containing the test data.
    :param variable_: string object containing the feature heading of the gdf (i.e. PROPERTY or PROP_TAG).
    :param dict_: dictionary object relevant to the input variable_
    :return gdf: output geo-dataframe object containing the test data with an updated variable_ feature.
    :return final_verified_tag_list: list object with status variables (i.e. VERIFIED, AUTO-CORRECTED or FAULTY)
    """
    print("="*50)
    print("=" * 50)
    print("=" * 50)
    final_verified_tag_list = []
    final_prop_code_list = []

    for prop_ in gdf.PROPERTY.unique():
        print('prop: ', prop_)
        # gdf filtered by:  prop_
        property_filter = gdf[gdf['PROPERTY'] == prop_]
        prop_code = dict_[prop_]
        prop_tag_list = property_filter[variable_].tolist()

        verified_tag_list = []
        prop_code_list = []
        for prop_tag in prop_tag_list:
            #print("prop_tag: ", prop_tag)
            #print("prop code: ", prop_code)
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

    # update feature variable_
    gdf[variable_] = final_prop_code_list

    return gdf, final_verified_tag_list


def create_property_name_set_fn(pastoral_estate):
    """ Create a set of property names from the Pastoral Estate shapefile path.

    :param pastoral_estate: string object containing the path to the Pastoral Estate shapefile.
    :return pastoral_estate_gdf: geo-dataframe object containing the Pastoral Estate.
    :return property_name_set: set object containing all of the property names within the Pastoral Estate.
    """
    pastoral_estate_gdf = gpd.read_file(pastoral_estate)
    property_name_list = pastoral_estate_gdf.PROPERTY.tolist()
    property_name_set = set(property_name_list)

    return pastoral_estate_gdf, property_name_set


def check_property_details_fn(pastoral_estate_gdf, gdf):
    """ Create two dictionaries (district and property code) based on property name.

    :param pastoral_estate_gdf: geo-dataframe containing the Pastoral Estate shapefile (command argument).
    :param gdf: input geo-dataframe object containing the feature specific test data.
    :return output_gdf: output geo-dataframe object containing the feature specific test data with updated DISTRICT and
    PROP_TAG features.
    :return property_name_set: set object containing all of the property names within the Pastoral Estate.
    :return district_verified_list: list object containing status variables based on any corrections made or errors
    identified within the DISTRICT column
    :return prop_tag_verified_list: list object containing status variables based on any corrections made or errors
    identified within the PROP_TAG column
    """
    prop_dist_dict = pastoral_estate_gdf.set_index('PROPERTY').to_dict()['DISTRICT']
    #print('prop_dist_dict: ', prop_dist_dict)
    prop_prop_code_dict = pastoral_estate_gdf.set_index('PROPERTY').to_dict()['PROP_TAG']
    #print('prop_prop_code_dict: ',prop_prop_code_dict)
    property_name_list = pastoral_estate_gdf.PROPERTY.tolist()
    property_name_set = set(property_name_list)

    gdf2, district_verified_list = pastoral_estate_check_fn(
        gdf, 'DISTRICT', prop_dist_dict)

    output_gdf, prop_tag_verified_list = pastoral_estate_check_fn(
        gdf2, 'PROP_TAG', prop_prop_code_dict)

    return output_gdf, property_name_set, district_verified_list, prop_tag_verified_list


def clean_source_fn(gdf):
    """

    :param gdf: geo-dataframe object
    :return gdf: processed geo-dataframe
    """
    source_list = gdf['SOURCE'].tolist()
    clean_source_list = []
    for i in source_list:
        if i == "NTG Rangelands Monitoring Branch":
            x = "NTG Rangeland Monitoring Branch"
            clean_source_list.append(x)
        else:
            clean_source_list.append(i)

    gdf['SOURCE'] = clean_source_list

    return gdf


def main_routine(gdf, feature_type, pastoral_estate):
    """ Check the features: PROPERTY, PROP_TAG, FEATGROUP AND FEATURE. Check for errors using existing data sets and
    rectify issues based on a variables levenshtein_ratio to known data. Add STATUS feature to geo-dataframe,
    by replacing the observation specific variable VERIFIED list by list.

    """

    pastoral_estate_gdf, property_name_set = create_property_name_set_fn(pastoral_estate)
    print('-' * 50)
    print('Let me check your feature attributes for obvious errors..........')
    # process the feature
    feature_list = []

    feature_list_ = gdf.PROPERTY.tolist()
    for i in feature_list_:
        n = i.upper()
        feature_list.append(n)

    gdf['PROPERTY'] = feature_list
    # Call the compare_features_against_data_sets function to check variables in column and replace when required
    # where possible.
    output_list, gdf, p_status_list = compare_features_against_data_sets_fn(
        feature_list, property_name_set, gdf, 'property')

    # Update feature with output list
    gdf['PROPERTY'] = output_list

    # call the identity_set_fn to determine the feature group and feature sets relevant to the feature type.
    feature_group_set, feature_set = identify_set_fn(feature_type)

    # process the feature group
    feature_list = gdf.FEATGROUP.tolist()
    feature_group_unique = gdf.FEATGROUP.unique().tolist()

    # Call the compare_features_against_data_sets function to check variables in column and replace when required
    # where possible.
    output_list, gdf, fg_status_list = compare_features_against_data_sets_fn(
        feature_list, feature_group_set, gdf, 'feature_group')

    # Update feature with output list
    gdf['FEATGROUP'] = output_list

    # process the feature
    feature_list = gdf.FEATURE.tolist()
    # Call the compare_features_against_data_sets function to check variables in column and replace when required
    # where possible.
    output_list, gdf, f_status_list = compare_features_against_data_sets_fn(
        feature_list, feature_set, gdf, 'feature')

    # Update feature with output list
    gdf['FEATURE'] = output_list


    # Call the check_property_details function to create two dictionaries based on property name.
    gdf, property_name_set, district_verified_list, prop_tag_verified_list = check_property_details_fn(
        pastoral_estate_gdf, gdf)

    gdf = clean_source_fn(gdf)

    # overwrite status list with comments for property, district, prop_tag, feature and feature group.
    result_list1 = [y if x == "VERIFIED" else x for x, y in zip(fg_status_list, f_status_list)]
    # print('len results 1: ', len(result_list1))
    result_list2 = [y if x == "VERIFIED" else x for x, y in zip(result_list1, p_status_list)]
    # print('len results 2: ', len(result_list2))
    result_list3 = [y if x == "VERIFIED" else x for x, y in zip(result_list2, district_verified_list)]
    # print('len results 3: ', len(result_list3))
    result_list = [y if x == "VERIFIED" else x for x, y in zip(result_list3, prop_tag_verified_list)]
    # print('len results: ', len(result_list))

    gdf["STATUS"] = result_list
    print(' Status column has been updated to reflect any changes or errors identified.')
    status_list = gdf.STATUS.unique().tolist()
    print('Status field contains: ')
    print(gdf.STATUS.value_counts())
    print('-'*30)

    faulty_gdf = gdf[gdf["STATUS"] ==  "ERROR"]
    good_gdf = gdf[gdf["STATUS"] != "ERROR"]


    if len(faulty_gdf.index) != 0 and len(good_gdf.index) == 0:
        print("ERROR_" * 50)
        print("ERROR_" * 50)
        print("ERROR_" * 50)
        print("ERROR_" * 50)
        print("ERROR_" * 50)
        print("=" * 50)
        print(f"100 % of observations has errors within the current {feature_type} shapefile......")
        print(faulty_gdf.PROPERTY.value_counts())

        print(f"The script may behave erratically; as such , the script has been terminated.")
        print("-"*30)
        print(f"Solution:")
        print("1. Find shapefile and correct issue.")
        print("2. Delete all data in for migration folders")
        print("3. Re run pipeline")



    # print(faulty_gdf["PROPERTY"].value_counts())

    # print("faulty_gdf: ", faulty_gdf.columns)
    # print("good_gdf: ", good_gdf)
    # print("feature: ", feature_type)

    # import sys
    # sys.exit()

    #return good_gdf, faulty_gdf #gdf
    return gdf


if __name__ == '__main__':
    main_routine()
