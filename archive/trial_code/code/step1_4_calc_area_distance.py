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


def add_currency_date(gdf):
    # extract date and update the date currency column (month / day / year)
    print(' - Enter the currency date as a string')
    date = datetime.now()
    format_date = date.strftime("%Y-%m-%d")
    print('format_date: ', format_date)

    # todo need to check property currency date +- 30 days exact data duplication.
    gdf["DATE_CURR"] = str(format_date)
    #print('gdf: ', gdf)
    return gdf


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


'''def previous_update_folder_structure(dir_list_item, transition_dir, year):
    print('l'*50)
    print('l' * 50)
    print('l' * 50)

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
        os.mkdir(dir_list_dir)'''


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
    print('step1_4 initiated')
    print('working on: ', feature_type)
    print('=' * 50)

    # todo add calculate area and length - may not work

    if feature_type == "polygons":
        gdf = calculate_area_km2_fn(gdf)
        print('Calculate area and length:')
        print(' - polygon area calculated.')

    elif feature_type == "paddocks":
        gdf = calculate_area_km2_fn(gdf)
        print('Calculate area and length:')
        print(' - paddocks area calculated.')

    elif feature_type == "lines":
        gdf = calculate_length_km2_fn(gdf)
        print('Calculate area and length:')
        print(' - lines length calculated.')

    else:
        pass

    print('=' * 50)
    gdf = add_currency_date(gdf)

    print('step 1_4 completed')
    return gdf, transition_dir, year, feature_type, status_text


if __name__ == '__main__':
    main_routine()
