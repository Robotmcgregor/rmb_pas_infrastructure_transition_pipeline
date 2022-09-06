# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_5_export_data.py
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
import os
import warnings
from glob import glob
import pandas as pd
import geopandas as gpd
from datetime import datetime
import fiona
import shutil

warnings.filterwarnings("ignore")


def convert_to_df_fn(previous_gdf, gdf):
    """ Convert geo-dataframe to a Pandas dataframe.

    :param previous_gdf: geo-dataframe object.
    :param gdf: geo-dataframe object.
    :return test_df: dataframe object.
    :return previous_df: dataframe object.
    """
    # convert geo-dataframe to a pandas df
    previous_df = pd.DataFrame(previous_gdf)

    # convert geo-dataframe to a pandas df
    test_df = pd.DataFrame(gdf)

    return test_df, previous_df


def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, dir_list_item, year):
    """ Export a copy of the property specific data that has been uploaded to the for_migration directory and previous
    upload directories.

    :param prop_curr_test: geo-dataframe containing new property specific new data.
    :param pastoral_districts_path: string object containing the path to the pastoral districts directory.
    :param dir_list_item: string object containing the Shapely feature type (i.e. points, lines)
    :param year: integer object containing the year YYYY.
    """

    for prop_ in prop_curr_test.PROPERTY.unique():

        print(" -- Cleaned data has been returned to ", prop_, " Server Upload directory.")
        prop_filter = prop_curr_test[prop_curr_test['PROPERTY'] == prop_]
        dist_ = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DISTRICT'].iloc[0]
        prop_code = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'PROP_TAG'].iloc[0]
        currency = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DATE_CURR'].iloc[0]
        #print("currency: ", currency)

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


        datetime_object = datetime.strptime(currency, "%Y-%m-%d %H:%M:%S")

        date_str = datetime_object.strftime("%Y%m%d_%H%M%S")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), dir_list_item)

        check_path = os.path.exists(output_path)

        if not check_path:
            os.mkdir(output_path)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter['UPLOAD'] = 'Transition'
        output = ("{0}\\{1}_Transition_{2}.shp".format(output_path, dir_list_item, date_str))
        print(' -- ', output)

        # if 'TRAN_DATE' in prop_filter.columns:
        #     prop_filter.drop(columns=['TRAN_DATE'], inplace=True)

        prop_filter.to_file(output, driver="ESRI Shapefile")

        prop_filter.to_csv("{0}\\{1}_Transition_{2}.csv".format(output_path, dir_list_item.title(), date_str))


def for_migration_export_shapefile_fn(test_data_gdf, for_migration_dir_path, feature_type, pt_schema):
    """ Export the finalised data to the FOR MIGRATION directory. Function will append data or save out depending on
    whether there is existing data in the FOR MIGRATION sub-directory.

    :param test_data_gdf: geo-dataframe object containing all new feature specific data for upload into the
    FOR MIGRATION directory.
    :param for_migration_dir_path: string object containing the path to the FOR MIGRATION directory.
    :param feature_type: string object containing the shapely object type (i.e. points, lines etc.)
    """

    output = "{0}\\Pastoral_Infra_{1}.shp".format(for_migration_dir_path, feature_type.title())
    check_file = os.path.exists(output)
    print(" - For Migration dataset updated.")

    if check_file:
        gdf = gpd.read_file(output, driver="ESRI shapefile")
        gdf2 = gdf.append(test_data_gdf)
        filtered_gdf_sort = gdf2.sort_values(["PROPERTY", "DATE_CURR"])
        filtered_gdf_sort.to_file(output, driver="ESRI Shapefile")
        print(' -- FOR MIGRATION data has been appended.....')
    else:

        test_gdf_sort = test_data_gdf.sort_values(["PROPERTY", "DATE_CURR"])
        test_gdf_sort.to_file(output, driver="ESRI Shapefile")
        print(' -- FOR MIGRATION data has exported.....')


def export_transition_fn(new_gdf, file_path, feature_type):
    """ Export the finalised data to the PREVIOUS TRANSFER directory. All test data nad previous transfer data has been
    proceeded through the scripts. As such, appending of data is required.

    :param gdf: geo-dataframe object containing all new feature specific data for upload into the
    PREVIOUS TRANSFER directory.
    :param previous_dir_path: string object containing the path to the FOR MIGRATION directory.
    :param feature_type: string object containing the shapely object type (i.e. points, lines etc.)
    """
    print("-"*50)
    print("- Append to previous transfer")
    # add a trans_date field (date_time object) from the date_curr field
    # print(new_gdf.dtypes)

    pt_gdf = fiona.open(file_path)
    pt_schema = pt_gdf.schema

    # open as geo-dataframe called transition_gdf
    prev_trans_gdf = gpd.read_file(file_path, geometry="geometry")
    print("Previous Transfer Shape: ", prev_trans_gdf.shape)
    print("New Data Shape: ", new_gdf.shape)
    gdf = prev_trans_gdf.append(new_gdf)
    print("Appended Previous Transfer Shape", gdf.shape)
    print(" - Previous Transfer dataset updated.")

    output_gdf_sort = gdf.sort_values(["PROPERTY", "DATE_CURR"], ascending=[True, True])
    output_gdf_sort.to_file(file_path, driver="ESRI Shapefile", schema = pt_schema)


def main_routine(new_gdf, feature_type, transition_dir, year, pastoral_districts_path):
    """ 1. Search for feature type specific data already in the transfer directory.

    """

    print('Searching for ', feature_type, ' data already in the transition directory....')
    previous_dir_path = os.path.join(transition_dir, "Previous_Transfer", feature_type.title())
    print(' - ',  previous_dir_path)
    for_migration_dir_path = os.path.join(transition_dir, "For_Migration", feature_type.title())

    if glob("{0}\\{1}".format(previous_dir_path, "Previous_Transfer_*_gda94.shp")):

        for file_path in glob("{0}\\{1}".format(previous_dir_path, "Previous_Transfer_*_gda94.shp")):
            print(' - Previous transfer shapefile located: ', file_path)
            # print(file_path)

            pt_schema = export_transition_fn(new_gdf, file_path, feature_type)

            property_export_shapefile_fn(new_gdf, pastoral_districts_path, feature_type, year)

            for_migration_export_shapefile_fn(new_gdf, for_migration_dir_path, feature_type, pt_schema)
    else:
        print("ERROR - No previous data exists - can not read in schema")
        import sys
        sys.exit()

if __name__ == '__main__':
    main_routine()
