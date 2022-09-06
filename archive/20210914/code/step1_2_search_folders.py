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

warnings.filterwarnings("ignore")


def property_path_fn(path):
    """ Create a path to all property sub-directories and return them as a list.

    :param path: string object containing the path to the Pastoral Districts directory.
    :return prop_list: list object containing the path to all property sub-directories.
    """
    # create a list of pastoral districts.
    dir_list = next(os.walk(path))[1]

    prop_list = []

    # loop through districts to get property name
    for district in dir_list:
        dist_path = os.path.join(path, district)

        property_dir = next(os.walk(dist_path))[1]

        # loop through the property names list
        for prop_name in property_dir:
            # join the path, district and property name to create a path to each property directory.
            prop_path = os.path.join(path, district, prop_name)
            # append all property paths to a list
            prop_list.append(prop_path)

    # print(prop_list)
    return prop_list


def upload_download_path_fn(prop_list):
    """ Create a path to to the Server Upload and Download sub-directories for each property and return them as a list.

    :param prop_list: list object containing the path to all property sub-directories.
    :return upload_list: list object containing the path to each properties Server_Upload sub-directory.
    :return download_list: list object containing the path to each properties Server_Upload sub-directory.
    """
    upload_list = []
    download_list = []
    for prop_path in prop_list:
        upload_path = os.path.join(prop_path, 'Infrastructure', 'Server_Upload')
        upload_list.append(upload_path)
        download_path = os.path.join(prop_path, 'Infrastructure', 'Server_Download')
        download_list.append(download_path)

    return upload_list, download_list


def assets_search_fn(search_criteria, folder):
    """ Searches through a specified directory "folder" for a specified search item "search_criteria".

    :param search_criteria: string object containing a search variable including glob wildcards.
    :param folder: string object containing the path to a directory.
    :return files: string object containing the path to any located files or "" if none were located.
    """
    path_parent = os.path.dirname(os.getcwd())
    assets_dir = (path_parent + '\\' + folder)

    files = ""
    file_path = (assets_dir + '\\' + search_criteria)
    for files in glob(file_path):
        # print(search_criteria, 'located.')
        pass

    return files


def shapely_check_geom_type_fn(files, feature_type):
    import fiona
    import shapely
    from shapely.geometry import shape

    #print('line 106 files: ', files)
    feature_type_dict = {'points': shapely.geometry.point.Point, 'lines': shapely.geometry.linestring.LineString,
                         'paddocks': shapely.geometry.polygon.Polygon,
                         'polygons': shapely.geometry.polygon.Polygon,
                         'multi_part': shapely.geometry.multipolygon.MultiPolygon}
    #print('feature_type: ', feature_type)
    shapely_geometry = feature_type_dict[feature_type]
    #print('shapely_geometry : ', shapely_geometry)

    shapefile = fiona.open(files)
    #print(shapefile.schema)
    first = next(iter(shapefile))
    #print('first: ', first)

    # extract dictionary value
    shp_geom = shape(first['geometry'])

    # compare expected geometry to actual geometry and return a boolean value
    if type(shp_geom) == shapely_geometry:
        true_false = True
        print(" - Correct")
    else:
        true_false = False
        print(" - Incorrect")

    return true_false, shp_geom


def geom_type(files, feature_type):
    print('geom type.......')

    import ogr
    shapefile_location = files
    shapefile = ogr.Open(shapefile_location)
    layer = shapefile.GetLayer()
    print(layer)

    for feature in layer:
        print('feature: ', feature)
        geometry = feature.GetGeometryRef()
        print('geom_type::::::::::', geometry)
        print(geometry.GetGeometryName())



def filter_transition_fn(gdf, files):
    if 'UPLOAD' in gdf.columns:
        # print('['*50)
        # print('files: ', files)
        # print(gdf)
        trans_gdf = gdf
        print('upload column identified')
        upload = False
        pass
    else:
        upload = True
        if 'STATUS' in gdf.columns:
            trans_gdf = gdf[(gdf["STATUS"] != "Transition") | (gdf["STATUS"] != "Migration")]
            #print('(gdf["STATUS"] != "Transition") | (gdf["STATUS"] != "Migration") identified')
        else:
            trans_gdf = gdf
            #print('(gdf["STATUS"] != "Transition") | (gdf["STATUS"] != "Migration") NOT identified')

    trans_gdf['STATUS'] = 'Transition_attempt'
    gdf.to_file(files)
    if 'NOTES' in trans_gdf.columns:
        trans_gdf = trans_gdf.drop(columns=['NOTES'])
    if 'OBJECTID' in trans_gdf.columns:
        trans_gdf = trans_gdf.drop(columns=['OBJECTID'])

    return trans_gdf, upload


def extract_paths_fn(upload_list, year, directory_list, status_text):
    """ Search through each server upload subdirectory within the root pastoral directory directory for line, point and
    polygon shapefiles, read them in as  geo-dataframe and append them to a list if UPLOAD column does not exists.

    :param upload_list: list object containing all server_uploads subdirectory paths (i.e. 1 per property).
    :param year: string object containing the year.
    :param directory_list: list object containing the feature types/subdirectory names (i.e. points, lines etc.)
    :return points_list: list object containing open point geo-dataframes located within the server_upload subdirectories.
    :return lines_list: list object containing open line geo-dataframes located within the server_upload subdirectories.
    :return poly_list: list object containing open polygons geo-dataframes located within the server_upload subdirectories.
    """

    # create three empty list to append results
    points_list = []
    lines_list = []
    poly_list = []
    paddocks_list = []
    files_list = []
    # loop through each property server upload subdirectory.
    for server_upload_path in upload_list:
        # loop though each item in the directory list (i.e. points, polygons, lines etc.)
        for feature_type in directory_list:

            direct = os.path.join(server_upload_path, str(year), feature_type)

            check_folder = os.path.isdir(direct)
            if check_folder:

                for files in glob(direct + '\\*.shp'):
                    print('files: ', files)

                    gdf = gpd.read_file(files)
                    if 'UPLOAD' not in gdf.columns:

                        files_list.append(files)
                        #print('files...........', files)

                        #files_list.append(files)
                        # print('feature type: ', feature_type)
                        # print('Files located: ', files)

                        # todo remove once testing has finished.
                        # os.remove(files)
                        # check geo_type
                        # Call the shapely check geom type function to verify the geometry of the shapefile.
                        #geom_type(files, feature_type)

                        true_false, shp_geom = shapely_check_geom_type_fn(files, feature_type)

                        if true_false:
                            # read file as geo-dataframe
                            gdf = gpd.read_file(files)
                            trans_gdf, upload = filter_transition_fn(gdf, files)

                            if upload:
                                # shutil.copy(files, output_dir)
                                # sort and store open geo-dataframe's to feature_type specific lists.
                                if feature_type == 'points':
                                    points_list.append(trans_gdf)

                                elif feature_type == 'lines':
                                    lines_list.append(trans_gdf)

                                elif feature_type == 'polygons':
                                    poly_list.append(trans_gdf)

                                elif feature_type == 'paddocks':
                                    poly_list.append(trans_gdf)

                                else:
                                    pass

                        else:
                            import shapely
                            print('Was filed as a : ', feature_type)
                            # todo check if polygons are multipart or not

                            # sort and store open geo-dataframe's to feature_type specific lists.
                            if type(shp_geom) == shapely.geometry.point.Point:
                                print(' - shapefile was a Point')
                                gdf = gpd.read_file(files)
                                trans_gdf, upload = filter_transition_fn(gdf, files)
                                if upload:
                                    points_list.append(trans_gdf)

                            elif type(shp_geom) == shapely.geometry.linestring.LineString:
                                print(' - shapefile was a Point')
                                gdf = gpd.read_file(files)
                                trans_gdf, upload = filter_transition_fn(gdf, files)
                                if upload:
                                    lines_list.append(trans_gdf)

                            else:
                                # paddocks and poly other can not be separated

                                gdf = gpd.read_file(files)
                                gdf['STATUS'] = 'Failed Geom'
                                gdf.to_file(files)
                                status_text.write('ERROR......status: failed - unknown geom (check sub-folder location \n')
                                status_text.write(files)

                    else:
                        print('Contains STATUS as a column and will NOT be processed...........')
                        print(' - ', files)

    print('files_list: ', files_list)
    # print(points_list, lines_list, poly_list, paddocks_list)
    return points_list, lines_list, poly_list, paddocks_list, status_text, files_list


def check_column_names_fn(df_list, asset_dir, file_end, status_text):
    """ Sort input dataframe into either the checked_list or faulty_list based on it having only the required column
    headings.

    :param df_list: list object containing open dataframes extracted from the pastoral districts directory.
    :param asset_dir: directory containing correct empty dataframe structures.
    :param file_end: string object containing the file name.
    :return checked_list: list object containing dataframes that matched.
    :return faulty_list: list objet containing dataframes that did not match.
    """
    print('accurate_df: ', '{0}\\shapefile\\{1}'.format(asset_dir, file_end))
    accurate_df = gpd.read_file('{0}\\shapefile\\{1}'.format(asset_dir, file_end), index_col=0)

    checked_list = []
    faulty_list = []
    # print(' The following geo-dataframes were located')
    # print(' - ', df_list)
    # print('Checking that the data has the correct column names')
    for test_df in df_list:
        # create two lists from the correctly formatted csv (accurate) and the data for upload (test)
        accurate_cols = accurate_df.columns.tolist()
        print('accurate_cols: ', accurate_cols)
        #todo - commented out 20210609

        # accurate_cols.remove("OBJECTID")
        test_cols = test_df.columns.tolist()
        print('test_cols: ', test_cols)
        # check that the columns have the same number of headers as the accurate version.
        if len(accurate_cols) == len(test_cols):
            print('There are the correct amount of columns.')
            status_text.write('Column lengths are the same: \n')
            status_text.write(' - test col:  ')
            status_text.write(str(len(test_cols)))
            status_text.write('Column lengths are the same: \n')
            status_text.write(' - accurate_cols:  ')
            status_text.write(str(len(accurate_cols)))

            # check that the column headers match the accurate version.
            if accurate_cols == test_cols:
                checked_list.append(test_df)
            else:
                if set(accurate_cols) == set(test_cols):
                    print('Error --- column names are correct; however, they are in the wrong order.')
                    print('Let me fix this for you..........')
                    test_df = test_df[accurate_cols]
                    print('Completed... Your welcome..')
                    checked_list.append(test_df)
                else:

                    # print("!" * 50)
                    # print(accurate_cols)
                    # print(test_cols)
                    print('Error --- column names are incorrect - data will not be uploaded.')
                    print(' - Expected column names: ', accurate_cols)
                    # print(' - Expected number of columns: ', len(accurate_cols))
                    print(' - Actual column names: ', test_cols)
                    # print(' - Actual number of columns: ', len(test_cols))
                    test_df['STATUS'] = "Incorrect column order"
                    faulty_list.append(test_df)

        else:
            test_df['STATUS'] = "Incorrect column header/order"
            faulty_list.append(test_df)
            print('Error --- dataframe length is incorrect - data will not be uploaded.')
            # print(' - Expected column names: ', accurate_cols)
            print(' - Expected number of columns: ', len(accurate_cols))
            # print(' - Actual column names: ', test_cols)
            print(' - Actual number of columns: ', len(test_cols))

            status_text.write(
                'Not all uploads have correct column names and lengths; as such, will not be uploaded. please check. \n')

    return checked_list, faulty_list, status_text


def dataframe_astypes_fn(df):
    """

    :param df:
    :return df:
    """
    # todo don't think this is required if using shapefile template
    df.astype({"FEATGROUP": 'object'}).dtypes
    df.astype({'FEATURE': 'object'}).dtypes
    df.astype({'LABEL': 'object'}).dtypes
    df.astype({'DATE_INSP': 'object'}).dtypes
    df.astype({'DATE_CURR': 'object'}).dtypes
    # df['DATEINSP'].to_datetime
    # df['DATECURR'].to_datetime
    df.astype({'DISTRICT': 'object'}).dtypes
    df.astype({'PROPERTY': 'object'}).dtypes
    df.astype({'PROP_TAG': 'object'}).dtypes
    df.astype({'SOURCE': 'object'}).dtypes
    df.astype({'MAPDISPLAY': 'object'}).dtypes
    df.astype({'CONFIDENCE': 'int32'}).dtypes
    if 'LENGTH_M' in df.columns:
        df.astype({'LENGTH_M': 'int32'}).dtypes
    if 'AREA_KM2' in df.columns:
        df.astype({'AREA_KM2': 'int32'}).dtypes

    if 'CONDITION' in df.columns:
        df.drop(columns=['CONDITION'], inplace=True)

    return df


def date_reformat_fn(gdf, column_header):
    insp_date_list = []
    for i in gdf[column_header]:
        # print(i)
        if '-' in i:
            year, month, day = i.split('-')
            new_date = '{0}-{1}-{2}'.format(year, month, day)
            # print(new_date)

        elif '/' in i:
            print('/////////////////')
            print(i)
            day, month, year = i.split('/')
            new_date = '{0}-{1}-{2}'.format(year, month, day)

        insp_date_list.append(new_date)

    print(insp_date_list)
    gdf[column_header] = insp_date_list

    return gdf


def concat_and_clean_df_fn(list_a):
    df_concat = pd.concat(list_a)
    # remove duplicates
    df_concat.drop_duplicates()
    df = pd.DataFrame(df_concat)
    # df_concat_ = dataframe_astypes_fn(df)

    # convert to a geo-dataframe
    gdf = gpd.GeoDataFrame(
        df, geometry='geometry')  # df_concat_

    # drop unwanted columns
    # gdf.drop(columns=['PROPERTY', 'PROP_TAG'])
    if 'NOTES' in gdf.columns:
        gdf.drop(columns=['NOTES'], inplace=True)
    if 'OBJECTID' in gdf.columns:
        gdf.drop(columns=['OBJECTID'], inplace=True)

    if 'LABEL' in gdf.columns:
        gdf.DATE_INSP.replace("Not recorded", "", inplace=True)

    return gdf


def compare_dataframes_fn(previous, input_data_gdf):
    """ Compare two dataframes for the same observations ignoring the index column and removing duplicates from the
    output geo-dataframe

    :param previous: geo-dataframe object containing previous upload data.
    :param input_data_gdf: geo-dataframe object containing the located data for upload.
    :return edited_data_fn: geo-dataframe containing the only new data.
    """
    print('previous data: ', previous.shape)
    print('input_data_gdf: ', input_data_gdf)
    # check for the same observations in the new data as in the previous data, and remove them from the new data.
    input_data_gdf2 = input_data_gdf.merge(previous, how='outer', indicator='merge_')

    #  filter df to new data (left_only)
    edited_data_df = input_data_gdf2[input_data_gdf2['merge_'] == 'left_only']
    edited_data_df['check'] = 'left'
    print('edited_data_df: ', edited_data_df.shape)
    edited_data_df.drop(columns=['merge_'], inplace=True)
    edited_data_df["STATUS"] = 'Unseen data'
    # todo remove
    edited_data_df.to_file(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs\edited_data_df.shp')

    #  filter df to previous data (left_only)
    existing_data_df = input_data_gdf2[input_data_gdf2['merge_'] != 'left_only']
    existing_data_df['check'] = 'right'
    print('existing_data_df: ', edited_data_df.shape)
    existing_data_df.drop(columns=['merge_'], inplace=True)
    existing_data_df["STATUS"] = 'Existing data'
    # todo remove
    existing_data_df.to_file(r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs\existing_data_df.shp')

    return edited_data_df, existing_data_df


def concat_list_fn(list_a, feature_type, year, direc, export_dir, transition_dir, status_text):
    """

    :param export_dir:
    :param direc:
    :param year:
    :param list_a: list object containing open dataframes.
    :param feature_type: string object containing the feature name.
    :return df_concat: geopandas dataframe object:
    """
    # concatenate list of dataframes assign None if list is empty
    input_data_gdf = concat_and_clean_df_fn(list_a)
    # print('=' * 50)

    print(' - All ', feature_type, ' have been concatenated into one dataframe.')

    if 'DELETE' in input_data_gdf.columns:
        print(' - Concatenated geo-dataframe contains ', len(input_data_gdf.index), ' observations.')
        removed_delete_gdf = input_data_gdf[input_data_gdf['DELETE'] == 0]
        print(' - Delete observations where DELETE = 1')
        print(' - ', (len(input_data_gdf.index) - len(removed_delete_gdf.index)), ' observations were deleted.')
        status_text.write('observations were deleted.')

    else:
        removed_delete_gdf = input_data_gdf
        status_text.write('observations were NOT deleted - all delete values were 0.')

    if len(removed_delete_gdf.index) > 0:
        status_text.write("Geo- dataframe had no observations following deletion.")
        # todo delete to_file
        """removed_delete_gdf.to_file(
            r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT\delete1_" + feature_type +
            ".shp", driver="ESRI Shapefile")"""
    else:
        removed_delete_gdf = None
        status_text.write("Geo-dataframe had no observations following deletion.")

    return removed_delete_gdf, status_text, feature_type


def concat_faulty_list_fn(list_a, feature_type, export_dir, status_text):
    """

    :param export_dir:
    :param direc:
    :param year:
    :param list_a: list object containing open dataframes.
    :param feature_type: string object containing the feature name.
    :return df_concat: geopandas dataframe object:
    """
    # concatenate list of dataframes assign None if list is empty
    print('=' * 50)
    if len(list_a) == 0:
        df_concat = None
    else:
        df_concat = pd.concat(list_a)
        df_concat["STATUS"] = "Column Error"
        # remove duplicates
        # convert to a geo-dataframe
        status_text.write("Column error identified:  ")

        gdf = gpd.GeoDataFrame(
            df_concat, geometry='geometry')
        # todo file outputs back to directory
        output = '{0}\\faulty\\{1}\\{2}_faulty.shp'.format(export_dir, feature_type, feature_type)
        print('output: ', output)
        gdf.to_file(output)
        status_text.write(output)

    return status_text


def line_length_fn(df, epsg_int, out_epsg_int):
    """ Calculate the length of each line from the geometry column using the epsg_int projection.

    :param df: dataframe object
    :param epsg_int: integer object containing the epsg number for the length calculation (Albers).
    :param out_epsg_int: integer object containing the epsg number for original coordinate system (GDA94).
    :return output_df:  dataframe object with the updated length.
    """
    # set crs to a projected coordinate system
    # choose appropriately for your location
    proj_df = df.to_crs(epsg=epsg_int)
    # print(output_df.crs)
    return proj_df


def polygon_area_fn(df, epsg_int, out_epsg_int):
    """ Calculate the area of each polygon from the geometry column using the epsg_int projection.

    :param df: dataframe object
    :param epsg_int: integer object containing the epsg number for the area calculation (Albers).
    :param out_epsg_int: integer object containing the epsg number for original coordinate system (GDA94).
    :return output_df:  dataframe object with the updated area.
    """
    proj_df = df.to_crs(epsg=epsg_int)
    proj_df["AREA_KM2"] = proj_df['geometry'].area / 10 ** 6

    output_df = proj_df.to_crs(epsg=out_epsg_int)

    return output_df


def open_template_fn(template_dir, migration, feature_type, gdf):
    template_file = "{0}\\Template_{1}.shp".format(template_dir, feature_type.title())
    template = gpd.read_file(template_file)
    # concatenate the final data to the template data
    merged_gdf = pd.concat([template, gdf], ignore_index=True)

    merged_gdf.to_file("{0}\\lines\\lines_gda94".format(migration), driver="ESRI Shapefile")


def sort_files_paths(files_list):
    delete_files_list = []
    for file in files_list:

        gdf = gpd.read_file(file)
        if 'UPLOAD' not in gdf.columns:
            delete_files_list.append(file)

    return delete_files_list


def main_routine(path, transition_dir, assets_dir, year, direc, export_dir, migration, asset_dir, directory_list,
                 status_text):
    """

    """
    template_dir = r"C:\Users\admrmcgr\pythonProject\rmb_infrastructure_upload\assets\Template_Shape"

    print('step1_2_search_folders.py INITIATED.')
    print('initiated....')
    # call the directory_path_fn function to create a path to all property sub-directories and return them as a list.
    prop_list = property_path_fn(path)
    print('prop_list: ', prop_list)
    print('-' * 50)
    # call the upload_download_path_fn function to create a path to to the Server Upload and Download sub-directories
    # for each property and return them as a list.
    upload_list, download_list = upload_download_path_fn(prop_list)
    print('upload_list: ', upload_list)
    print('download_list: ', download_list)

    # call the extract_paths_fn function to search through each server upload subdirectory within the root pastoral
    # directory directory for line, point and polygon shapefiles, read them in as  geo-dataframe and append them to a
    # list.
    print("Checking that the shapefile geometry is accurate.....")
    points_list, lines_list, poly_list, paddocks_list, status_text, files_list = extract_paths_fn(upload_list, year,
                                                                                                  directory_list,
                                                                                                  status_text)

    delete_files_list = sort_files_paths(files_list)
    print('-' * 50)
    print('delete_files_list: ', delete_files_list)
    print('-' * 50)

    # ---------------------------------------------- POINTS ------------------------------------------------------------
    print('=' * 50)
    print('POINTS')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    print('Checking column names and lengths....')
    checked_points_list, faulty_points_list, status_text = check_column_names_fn(points_list, assets_dir,
                                                                                 'Pastoral_Infra_Points_Template.shp',
                                                                                 status_text)

    print(' - Correct dataframes: ', len(checked_points_list))
    print(' - Incorrect dataframes: ', len(faulty_points_list))

    # --------------------------------------------------- LINES --------------------------------------------------------
    print('-' * 50)
    print('LINES')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    checked_lines_list, faulty_lines_list, status_text = check_column_names_fn(lines_list, assets_dir,
                                                                               'Pastoral_Infra_Lines_Template.shp',
                                                                               status_text)

    print(' - Correct dataframes: ', len(checked_lines_list))
    print(' - Incorrect dataframes: ', len(faulty_lines_list))
    # print(checked_lines_list)

    # ---------------------------------------------------- POLYGONS ----------------------------------------------------

    print('-' * 50)
    print('POLYGONS')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    checked_polygons_list, faulty_polygons_list, status_text = check_column_names_fn(poly_list, assets_dir,
                                                                                     'Pastoral_Infra_Polygons_Template.shp',
                                                                                     status_text)

    print(' - Correct dataframes: ', len(checked_polygons_list))
    print(' - Incorrect dataframes: ', len(faulty_polygons_list))

    # ---------------------------------------------------- PADDOCKS ----------------------------------------------------

    print('-' * 50)
    print('PADDOCKS')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    checked_paddocks_list, faulty_paddocks_list, status_text = check_column_names_fn(paddocks_list, assets_dir,
                                                                                     'Pastoral_Infra_Paddocks_Template.shp',
                                                                                     status_text)

    print(' - Correct dataframes: ', len(checked_paddocks_list))
    print(' - Incorrect dataframes: ', len(faulty_paddocks_list))
    print('=' * 50)
    print('column names have been checked......')
    print('=' * 50)
    # ------------------------------------------------------------------------------------------------------------------

    # verify that the checked points file list contains a result
    concat_list = []
    feature_type_list = []

    if len(checked_points_list) > 0:
        print('Processing points.....')
        print('There are ', len(checked_points_list), ' geo-dataframes')
        points_removed_delete_gdf, status_text, feature_type = concat_list_fn(checked_points_list, 'points', year, direc,
                                                                       export_dir, transition_dir,
                                                                       status_text)

        concat_list.append(points_removed_delete_gdf)
        feature_type_list.append(feature_type)


    if len(faulty_points_list) > 0:
        status_text = concat_faulty_list_fn(faulty_points_list, 'points', export_dir, status_text)

    if len(checked_lines_list) > 0:
        print('Processing lines.....')
        print('There are ', len(checked_lines_list), ' geo-dataframes')
        lines_removed_delete_gdf, status_text, feature_type = concat_list_fn(checked_lines_list, 'lines', year, direc,
                                                                       export_dir, transition_dir, status_text)

        concat_list.append(lines_removed_delete_gdf)
        feature_type_list.append(feature_type)


    if len(faulty_lines_list) > 0:
        status_text = concat_faulty_list_fn(faulty_points_list, 'lines', export_dir, status_text)


    if len(checked_polygons_list) > 0:
        print('Processing polygons.....')
        print('There are ', len(checked_polygons_list), ' geo-dataframes')
        polygons_removed_delete_gdf, status_text, feature_type = concat_list_fn(checked_polygons_list, 'polygons', year, direc,
                                                                       export_dir, transition_dir,
                                                                       status_text)
        concat_list.append(polygons_removed_delete_gdf)
        feature_type_list.append(feature_type)


    if len(faulty_polygons_list) > 0:
        status_text = concat_faulty_list_fn(faulty_points_list, 'polygons', export_dir, status_text)

    if len(checked_paddocks_list) > 0:
        print('Processing paddocks.....')
        print('There are ', len(checked_paddocks_list), ' geo-dataframes')
        paddocks_removed_delete_gdf, status_text, feature_type = concat_list_fn(checked_paddocks_list, 'paddocks', year, direc,
                                                                       export_dir, transition_dir,
                                                                       status_text)
        concat_list.append(paddocks_removed_delete_gdf)
        feature_type_list.append(feature_type)

    if len(faulty_paddocks_list) > 0:
        status_text = concat_faulty_list_fn(faulty_points_list, 'paddocks', export_dir, status_text)


    return status_text, delete_files_list, concat_list, feature_type_list


if __name__ == '__main__':
    main_routine()
