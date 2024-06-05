# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_6_export_faulty_data.py
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
import warnings
from glob import glob
import geopandas as gpd
warnings.filterwarnings("ignore")


def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, dir_list_item, year, date_str):
    """ Export a copy of the property specific FAULTY data that has NOT been uploaded to the for_migration directory OR
    the previous upload directories.

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
        #currency = prop_curr_test.loc[prop_curr_test['PROPERTY'] == prop_, 'DATE_CURR'].iloc[0]
        print("CURRENCY DATE - faulty: ", currency)

        property_name = "{0}_{1}".format(prop_code.lower(), prop_.replace(' ', '_').lower())
        dist = dist_.replace(' ', '_')

        if dist == 'northern_alice_springs':
            district = 'northern_alice'
        elif dist == 'southern_alice_springs':
            district = 'southern_alice'
        elif dist == 'victoria_river':
            district = 'vrd'
        else:
            district = dist

        #datetime_object = datetime.strptime(currency, "%Y-%m-%d") # %H:%M:%S")
        #date_str = datetime_object.strftime("%Y%m%d") #_%00%00%00")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'infrastructure', 'server_upload',
                                   str(year), dir_list_item)

        check_path = os.path.exists(output_path)

        if not check_path:
            os.mkdir(output_path)

        prop_filter = prop_filter.replace(['Not Recorded'], "")
        prop_filter['UPLOAD'] = 'Transition'
        output = ("{0}\\{1}_transition_{2}.shp".format(output_path, dir_list_item, date_str))

        prop_filter.to_file(output, driver="ESRI Shapefile")

        prop_filter.to_csv("{0}\\{1}_Faulty_{2}.csv".format(output_path, dir_list_item, date_str))
        print(' -- A copy of FAULTY data has been exported to the property directory '
              '  --- ', output)


def main_routine(year, pastoral_districts_path, primary_export_dir, directory_list, faulty_gdf_list, date_str, datetime_object):
    """ This script controls the distribution of cleaned geo-dataframes in the transition, property and migration lists.
    Workflow is manages by the value relative to the property name in the status dictionary.
    """

    print('=' * 50)
    print("Faulty data")
    # print("directory_list: ", directory_list)

    for feature_type in directory_list:
        # print("Feature_Type: ", feature_type)

        n = os.path.join(primary_export_dir, 'faulty', feature_type)
        #print(n)
        for file in glob(os.path.join(n, "*{0}.shp".format(feature_type))):
            gdf = gpd.read_file(file)

            property_export_shapefile_fn(gdf, pastoral_districts_path, feature_type, year, date_str)

    if len(faulty_gdf_list) >0 :
        for faulty in faulty_gdf_list:
            print(faulty)

            n = os.path.join(primary_export_dir, 'faulty') #, feature_type)
            print(n)

            property_export_shapefile_fn(gdf, pastoral_districts_path, feature_type, year, date_str)
            ##TODO delete
            import sys
            sys.exit()




if __name__ == '__main__':
    main_routine()
