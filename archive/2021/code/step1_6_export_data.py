# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_6_export_data.py
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
        currency = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'TRAN_DATE'].iloc[0]

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

        # datetime_object = datetime.strptime(currency, "%Y-%m-%d %H:%M:%S").date()
        date_str = currency.strftime("%Y%m%d_%H%M%S")
        #print('date_str: ', date_str)

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
                                   str(year), dir_list_item)

        check_path = os.path.exists(output_path)

        if not check_path:
            os.mkdir(output_path)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter['UPLOAD'] = 'Transition'
        output = ("{0}\\{1}_transition_{2}.shp".format(output_path, dir_list_item, date_str))
        #print(' -- ', output)

        if 'TRAN_DATE' in prop_filter.columns:
            prop_filter.drop(columns=['TRAN_DATE'], inplace=True)

        prop_filter.to_file(output, driver="ESRI Shapefile")

        prop_filter.to_csv("{0}\\{1}_transition_{2}.csv".format(output_path, dir_list_item, date_str))


def for_migration_export_shapefile_fn(test_data_gdf, for_migration_dir_path, feature_type):
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

    print("test: ", len(test_data_gdf))
    if check_file:
        gdf = gpd.read_file(output, driver="ESRI shapefile")
        print("gdf: ", len(gdf))
        gdf2 = gdf.append(test_data_gdf)
        print('appending existing for migration with new data..')
        print(' - for migration: ', len(gdf.index))
        print(' - new data: ', len(test_data_gdf.index))
        print(' - appended data: ', len(gdf2.index))
        filtered_gdf = filter_previous_data_latest_currency_date_fn(gdf2)
        del filtered_gdf['TRAN_DATE']

        filtered_gdf_sort = filtered_gdf.sort_values(["PROPERTY", "DATE_CURR"])
        filtered_gdf_sort.to_file(output, driver="ESRI Shapefile")
        # print(' -- 3. FOR MIGRATION data has been appended.....')
    else:
        # print(' -- 3. FOR MIGRATION data has exported.....')
        del test_data_gdf['TRAN_DATE']
        test_gdf_sort = test_data_gdf.sort_values(["PROPERTY", "DATE_CURR"])
        test_gdf_sort.to_file(output, driver="ESRI Shapefile")


def export_transition_fn(gdf, previous_dir_path, feature_type):
    """ Export the finalised data to the PREVIOUS TRANSFER directory. All test data nad previous transfer data has been
    proceeded through the scripts. As such, appending of data is required.

    :param gdf: geo-dataframe object containing all new feature specific data for upload into the
    PREVIOUS TRANSFER directory.
    :param previous_dir_path: string object containing the path to the FOR MIGRATION directory.
    :param feature_type: string object containing the shapely object type (i.e. points, lines etc.)
    """
    print(" - Previous Transfer dataset updated.")

    if "TRAN_DATE" in gdf.columns:
        gdf.drop(columns=["TRAN_DATE"], inplace=True)

    output_gdf_sort = gdf.sort_values(["PROPERTY", "DATE_CURR"], ascending=[True, True])


    output_gdf_sort.to_file("{0}\\{1}{2}{3}".format(previous_dir_path, "previous_transfer_", feature_type, "_gda94.shp"),
                driver="ESRI Shapefile")


def filter_previous_data_latest_currency_date_fn(gdf):
    """ Filter geo-dataframe based on:
        1. property
        2. latest TRAN_DATE (currency date and time).
        Outputs are concatenated into one geo-dataframe.

    :param gdf: geo-dataframe object containing all new data (test) for the FOR MIGRATION directory.
    :return output_gdf: geo-dataframe object containing only the latest TRAN_DATE per property.
    """

    gdf['TRAN_DATE'] = pd.to_datetime(gdf['DATE_CURR'])

    final_list = []
    for prop_ in gdf.PROPERTY.unique():
        prop_filtered_gdf = gdf[gdf['PROPERTY'] == prop_]  # used to be previous_df
        # print('-length prop filtered: ', len(prop_filtered_gdf.index))

        # determine the latest currency date.
        date_list = []
        for i in prop_filtered_gdf.TRAN_DATE.unique():
            # print(prop_, ': --', i)
            date_list.append(i)

        latest_date = (max(date_list))
        # print(" - ", prop_, " - latest_date: ", latest_date)

        # filter the dataframe by the latest recorded curr_date
        filtered_gdf = prop_filtered_gdf[prop_filtered_gdf['TRAN_DATE'] == latest_date]  # used to be previous_df
        #print(' -- filtered_gdf length: ', len(filtered_gdf.index))

        final_list.append(filtered_gdf)

    output_gdf = pd.concat(final_list)
    print(output_gdf.columns)
    output_gdf_sort = output_gdf.sort_values(["PROPERTY", "DATE_CURR"], ascending=[True, True])
    return output_gdf_sort
    #return output_gdf


def main_routine(status_text, transition_dir, year, pastoral_districts_path, feature_type, status_dict, t_list, p_list,
                 m_list):
    """ This script controls the distribution of cleaned geo-dataframes in the transition, property and migration lists.
    Workflow is manages by the value relative to the property name in the status dictionary.
    """

    # print('='*50)
    # print('LOCATION OF PREVIOUS TRANSITION DIRECTORY: ', os.path.join(transition_dir, str(year), "previous_transfer"))
    # print('LOCATION OF MIGRATION DIRECTORY: ', os.path.join(transition_dir, str(year), "for_migration"))
    previous_dir_path = os.path.join(transition_dir, "Previous_Transfer", feature_type)


    for_migration_dir_path = os.path.join(transition_dir, "For_Migration", feature_type)
    # print('for_migration_dir_path: ', for_migration_dir_path)

    if len(t_list) > 0:
        t_df = pd.concat(t_list)
        t_gdf = gpd.GeoDataFrame(t_df, geometry='geometry')

    if len(p_list) > 0:
        p_df = pd.concat(p_list)
        p_gdf = gpd.GeoDataFrame(p_df, geometry='geometry')

    if len(m_list) > 0:
        m_df = pd.concat(m_list)
        m_gdf = gpd.GeoDataFrame(m_df, geometry='geometry')

    ft_list = []
    fp_list = []
    fm_list = []

    for key, value in status_dict.items():
        # print('=' * 50)
        # print('property: ', key)
        # print('status: ', value)
        if value == 'No transition data':
            prop_filter_t = t_gdf[t_gdf['PROPERTY'] == key]
            ft_list.append(prop_filter_t)
            prop_filter_p = p_gdf[p_gdf['PROPERTY'] == key]
            fp_list.append(prop_filter_p)
            prop_filter_m = m_gdf[m_gdf['PROPERTY'] == key]
            fm_list.append(prop_filter_m)

        elif value == 'No before, test NOT after':
            prop_filter_t = t_gdf[t_gdf['PROPERTY'] == key]
            ft_list.append(prop_filter_t)
            prop_filter_p = p_gdf[p_gdf['PROPERTY'] == key]
            fp_list.append(prop_filter_p)
            prop_filter_m = m_gdf[m_gdf['PROPERTY'] == key]
            fm_list.append(prop_filter_m)

        elif value == 'No before, test EQUALS after':
            prop_filter_t = t_gdf[t_gdf['PROPERTY'] == key]
            ft_list.append(prop_filter_t)
            prop_filter_p = p_gdf[p_gdf['PROPERTY'] == key]
            fp_list.append(prop_filter_p)
            prop_filter_m = m_gdf[m_gdf['PROPERTY'] == key]
            fm_list.append(prop_filter_m)

        elif value == 'Before, test NOT after':
            prop_filter_t = t_gdf[t_gdf['PROPERTY'] == key]
            ft_list.append(prop_filter_t)
            prop_filter_p = p_gdf[p_gdf['PROPERTY'] == key]
            fp_list.append(prop_filter_p)
            prop_filter_m = m_gdf[m_gdf['PROPERTY'] == key]
            fm_list.append(prop_filter_m)

        elif value == 'Property not in test':
            prop_filter_t = t_gdf[t_gdf['PROPERTY'] == key]
            ft_list.append(prop_filter_t)
            prop_filter_p = p_gdf[p_gdf['PROPERTY'] == key]
            fp_list.append(prop_filter_p)
            prop_filter_m = m_gdf[m_gdf['PROPERTY'] == key]
            fm_list.append(prop_filter_m)

        elif value == 'Before, test EQUALS after':
            prop_filter_t = t_gdf[t_gdf['PROPERTY'] == key]
            ft_list.append(prop_filter_t)
            prop_filter_p = p_gdf[p_gdf['PROPERTY'] == key]
            fp_list.append(prop_filter_p)
            prop_filter_m = m_gdf[m_gdf['PROPERTY'] == key]
            fm_list.append(prop_filter_m)

        else:
            print('ERROR '*20)
            print('script not complete')
            import sys
            sys.exit()
    print("EXPORTING DATA......")
    if len(ft_list) > 0:
        ft_df = pd.concat(ft_list)
        ft_gdf = gpd.GeoDataFrame(ft_df, geometry='geometry')
        export_transition_fn(ft_gdf, previous_dir_path, feature_type)

    if len(fp_list) > 0:
        fp_df = pd.concat(fp_list)
        fp_gdf = gpd.GeoDataFrame(fp_df, geometry='geometry')
        property_export_shapefile_fn(fp_gdf, pastoral_districts_path, feature_type, year)

    if len(fm_list) > 0:
        fm_df = pd.concat(fm_list)
        fm_gdf = gpd.GeoDataFrame(fm_df, geometry='geometry')
        for_migration_export_shapefile_fn(fm_gdf, for_migration_dir_path, feature_type)




    return status_text


if __name__ == '__main__':
    main_routine()
