# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_4_area_length_currency_date.py
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

from datetime import datetime
import warnings
warnings.filterwarnings("ignore")


def calculate_area_km2_fn(gdf):
    """ Calculate the area in Australian Albers.

    :param gdf: polygon geo-dataframe object in GDA94 geographics.
    :return output_gdf: polygon geo-dataframe object in GDA94 geographics with Australian Albers area calculated and
    overwritten.

    """
    gdf2 = gdf.copy()
    gdf3 = gdf2.to_crs(epsg=3577)
    gdf3["AREA_KM2"] = round((gdf3["geometry"].area / 10 ** 6), 1)
    gdf4 = gdf3.to_crs(epsg=4283)

    return gdf4


def calculate_length_km2_fn(gdf):
    """ Calculate the length in Australian Albers.

    :param gdf: line geo-dataframe object in GDA94 geographics.
    :return output_gdf: line geo-dataframe object in GDA94 geographics with Australian Albers length calculated and
    overwritten.
    """

    gdf2 = gdf.copy()

    print("gdf2.columns: ", gdf2.columns)

    gdf3 = gdf2.to_crs(epsg=3577)

    length_fault_list = []
    for g in gdf3.geometry:
        print(g)
        try:
            #print("length: ", round(g.length))
            print("-" * 20)
        except:
            print("error" * 50)
            var_ = g.PROPERTY
            length_fault_list.append(var_)

    if len(length_fault_list) > 0:
        length_fault_set = set(length_fault_list)
        print("Faulty length fields in the following properties: ", length_fault_set)
        import sys
        sys.exit()

    output_gdf = gdf3.to_crs(epsg=4283)

    return output_gdf


def add_currency_date(gdf):
    """ Calculate the current date and time and append to geo-dataframe under column heading TRAN_DATE.
    Convert date time to string and update DATE_CURR for geo-dataframe export.

    :param gdf: geo-dataframe object containing test data
    :return gdf: geo-dataframe object containing test data with the updated currency date.
    :return format_date: string object containing the date current date and time.
    """

    date_time = datetime.now()
    #gdf["TRAN_DATE"] = date_time
    format_date = date_time.strftime("%Y-%m-%d %H:%M:%S")
    gdf["DATE_CURR"] = str(format_date)

    return gdf, format_date


def main_routine(gdf, feature_type, transition_dir, year):
    """ Calculate the length of lines and the area of polygons and add the currency date and time.
    """

    # print("gdf: ", gdf)
    # print(gdf.crs)

    if feature_type == "polygons":
        gdf = calculate_area_km2_fn(gdf)
        print('Calculate area and length:')
        print(' - polygon area calculated.')

    elif feature_type == "paddocks":
        gdf = calculate_area_km2_fn(gdf)
        print('Calculate area and length:')
        print(' - paddocks area calculated.')

    elif feature_type == "lines":

        print(gdf.FEATURE.unique())
        # drop Fenceline Open and Fenceline Neighbour
        gdf_ = gdf[gdf["FEATURE"] != "Fenceline Open"]
        print("gdf_: ", gdf_.shape)
        gdf__ = gdf_[gdf_["FEATURE"] != "Fenceline Neighbour"]
        print("gdf__: ", gdf__.shape)
        gdf__.to_file(r"P:\Pipelines\Output\rmb_infrastructure_transition\error.shp", driver="ESRI Shapefile")
        gdf = calculate_length_km2_fn(gdf__)
        print('Calculate area and length:')
        print(' - lines length calculated.')

    else:
        pass

    print('=' * 50)
    gdf, format_date = add_currency_date(gdf)


    return gdf, transition_dir, year, feature_type


if __name__ == '__main__':
    main_routine()
