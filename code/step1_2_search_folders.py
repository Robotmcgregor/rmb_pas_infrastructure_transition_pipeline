# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_2_search_folders.py
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
from shapely.geometry import shape

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
    """ Check that the predicted shapefile (based on it location within the upload sub-directory) is the correct
    Shapely shape (shapefile geometry).

    :param files: path object to the current shapefile within the server upload sub-directory within the Pastoral
    District directory.
    :param feature_type: string object containing the type of shapefile the geo-dataframe is expected to be, based on
    folder structure.
    :return true_false: boolean object confirming that the shapefile has been assigned the correct Shapely shape.
    :return shp_geom: Shapely shape relevant to the current shapefile.
    """
    import fiona
    import shapely
    from shapely.geometry import shape

    feature_type_dict = {'points': shapely.geometry.point.Point,
                         'lines': shapely.geometry.linestring.LineString,
                         'paddocks': shapely.geometry.polygon.Polygon,
                         'polygons': shapely.geometry.polygon.Polygon,
                         'multi_part': shapely.geometry.multipolygon.MultiPolygon}
    # extract shapely object from the feature_type_dict
    shapely_geometry = feature_type_dict[feature_type]

    shapefile = fiona.open(files)
    # print(shapefile.schema)
    first = next(iter(shapefile))
    # print('first: ', first)

    # extract dictionary value
    shp_geom = shape(first['geometry'])

    # compare expected geometry to actual geometry and return a boolean value
    if type(shp_geom) == shapely_geometry:
        true_false = True

    else:
        true_false = False


    return true_false, shp_geom


def filter_transition_fn(gdf, files):
    """ Filter out Transition or Migration from the STATUS feature and add 'Transition attempt'.
    Additionally, remove features NOTES and OBJECTID.

    :param gdf: input geo-dataframe object.
    :param files: string object containing the path to the current geo-dataframe.
    :return trans_gdf: output geo-dataframe following function processing.
    :return upload: boolean object whether the geo-dataframe contains the feature UPLOAD..
    """
    if 'UPLOAD' in gdf.columns:

        trans_gdf = gdf
        upload = False
        pass
    else:
        upload = True
        if 'STATUS' in gdf.columns:
            trans_gdf = gdf[(gdf["STATUS"] != "Transition") | (gdf["STATUS"] != "Migration")]
        else:
            trans_gdf = gdf

    trans_gdf['STATUS'] = 'Transition_attempt'
    gdf.to_file(files)
    if 'NOTES' in trans_gdf.columns:
        trans_gdf = trans_gdf.drop(columns=['NOTES'])
    if 'OBJECTID' in trans_gdf.columns:
        trans_gdf = trans_gdf.drop(columns=['OBJECTID'])

    return trans_gdf, upload


def double_check_polygon_fn(gdf, feature_type):
    """ Verify that shapely feature type polygon is either paddock or polygon.

    :param gdf: geo-dataframe object containing feature specific (POLYGON) observations
    :param feature_type: string object containing the type of shapefile the geo-dataframe is expected to be, based on
    folder structure.
    :return correct_feature: verified string object containing the accurate polygon feature type
    (i.e. polygon or paddock).
    """
    print('+' * 50)
    print('Double check that the Polygons are paddocks or poly_other')
    print('Checking if shapefile is a .......')
    print(' - ', feature_type)
    feature_list = gdf.FEATURE.unique().tolist()

    if "Paddock" in feature_list:
        print('- should be paddocks')
        correct_feature = "paddocks"
    else:
        print('- should be polygon')
        correct_feature = "polygons"

    print("correct_feature: ", correct_feature)
    return correct_feature


def extract_paths_fn(upload_list, year, directory_list):
    """ Search through each server upload subdirectory within the root pastoral directory directory for line, point and
    polygon shapefiles, read them in as  geo-dataframe and append them to a list if UPLOAD column does not exists.

    :param upload_list: list object containing all server_uploads subdirectory paths (i.e. 1 per property).
    :param year: string object containing the year.
    :param directory_list: list object containing the feature types/subdirectory names (i.e. points, lines etc.)
    :return points_list: list object containing open point geo-dataframes located within the server_upload
    subdirectories.
    :return lines_list: list object containing open line geo-dataframes located within the server_upload subdirectories.
    :return poly_list: list object containing open polygons geo-dataframes located within the server_upload
    subdirectories.
    :return paddocks_list: list object containing open polygons geo-dataframes located within the server_upload
    subdirectories.
    :return files_list: list object containing file paths to all shapefiles within the upload subdirectory within the
    Pastoral Districts directory.
    """

    # create three empty list to append results
    points_list = []
    lines_list = []
    poly_list = []
    paddocks_list = []
    files_list = []
    # print('paddocks_list: ', len(paddocks_list))
    # loop through each property server upload subdirectory.

    for server_upload_path in upload_list:
        # loop though each item in the directory list (i.e. points, polygons, lines etc.)
        for feature_type in directory_list:

            # create a path for to a properties feature type sub-directory within the server upload directory
            direct = os.path.join(server_upload_path, str(year), feature_type)

            check_folder = os.path.isdir(direct)

            if check_folder:

                for files in glob(direct + '\\*.shp'):

                    # print("Located: ", files)
                    #
                    gdf = gpd.read_file(files)
                    #
                    # # # check that a file is only for one property.
                    # prop_list = gdf["PROPERTY"].unique().tolist()
                    # print("looking at data from: ", prop_list)
                    # print(gdf.columns)
                    # if len(prop_list) > 0:
                    #     print('single property')
                    # else:
                    #     print('more than one')
                    #
                    #
                    # import sys
                    # sys.exit()

                    # check that the shapefile located has not previously been transitioned based on the UPLOAD column.
                    if 'UPLOAD' not in gdf.columns:

                        files_list.append(files)

                        # Call the shapely_check_geom_type function to check that the predicted shapefile (based on it's
                        # location within the upload sub-directory) is the correct Shapely shape (shapefile geometry).
                        true_false, shp_geom = shapely_check_geom_type_fn(files, feature_type.lower())

                        if true_false:
                            # read file as geo-dataframe
                            gdf = gpd.read_file(files)
                            # Call the filter_transition function to filter out Transition or Migration forms the STATUS
                            # feature and add 'Transition attempt'. Additionally, remove features NOTES and OBJECTID.
                            trans_gdf, upload = filter_transition_fn(gdf, files)

                            # print(trans_gdf)
                            # 
                            # # # check that a file is only for one property.
                            # prop_list = trans_gdf["PROPERTY"].unique().tolist()
                            # print("looking at data from: ", prop_list)
                            # print(gdf.columns)
                            # if len(prop_list) > 0:
                            #     print('single property')
                            # else:
                            #     print('more than one property is listed in ')
                            #     import sys
                            #     sys.exit()
                            # 
                            # 
                            # 
                            # import sys
                            # sys.exit()


                            if upload:
                                # shutil.copy(files, output_dir)
                                # sort and store open geo-dataframe's to feature_type specific lists.
                                if feature_type == 'Points':
                                    points_list.append(trans_gdf)

                                elif feature_type == 'Lines':
                                    lines_list.append(trans_gdf)

                                elif feature_type == 'Polygons':
                                    print('feature type is: ', feature_type)
                                    correct_feature = double_check_polygon_fn(trans_gdf, feature_type)
                                    if feature_type == correct_feature.title():
                                        poly_list.append(trans_gdf)
                                    else:
                                        paddocks_list.append(trans_gdf)

                                elif feature_type == 'Paddocks':
                                    print('feature type is: ', feature_type)
                                    correct_feature = double_check_polygon_fn(trans_gdf, feature_type)

                                    if feature_type == correct_feature.title():
                                        paddocks_list.append(trans_gdf)

                                    else:
                                        poly_list.append(trans_gdf)

                                else:
                                    pass

                        else:
                            import shapely
                            print('Was filed as a : ', feature_type)

                            # sort and store open geo-dataframe's to feature_type specific lists.
                            if type(shp_geom) == shapely.geometry.point.Point:
                                print(' - shapefile was a Point')
                                gdf = gpd.read_file(files)
                                # Call the filter_transition function to filter out Transition or Migration form the
                                # STATUS feature and add 'Transition attempt'. Additionally, remove features NOTES and
                                # OBJECTID.
                                trans_gdf, upload = filter_transition_fn(gdf, files)
                                if upload:
                                    points_list.append(trans_gdf)

                            elif type(shp_geom) == shapely.geometry.linestring.LineString:
                                print(' - shapefile was a Line')
                                gdf = gpd.read_file(files)
                                # Call the filter_transition function to filter out Transition or Migration form the
                                # STATUS feature and add 'Transition attempt'. Additionally, remove features NOTES and
                                # OBJECTID.
                                trans_gdf, upload = filter_transition_fn(gdf, files)
                                if upload:
                                    lines_list.append(trans_gdf)

                            else:
                                # paddocks and poly other can not be separated
                                print('ERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
                                print("A polygon is loaded in the wrong sub-folder I can't differentiate with 100% "
                                      "certainty")
                                print(' - ', files)
                                gdf = gpd.read_file(files)

                                gdf['STATUS'] = 'Failed Geom'
                                gdf.to_file(files)
                                print('goodbye')
                                import sys
                                sys.exit()

                    else:
                        pass

    return points_list, lines_list, poly_list, paddocks_list, files_list


def check_column_names_fn(df_list, asset_dir, file_end):
    """ Sort input dataframe into either the checked_list or faulty_list based on it having only the required column
    headings.

    :param df_list: list object containing open dataframes extracted from the pastoral districts directory.
    :param asset_dir: directory containing correct empty dataframe structures.
    :param file_end: string object containing the file name.
    :return checked_list: list object containing dataframes that matched.
    :return faulty_list: list objet containing dataframes that did not match.
    """

    accurate_df = gpd.read_file('{0}\\shapefile\\{1}'.format(asset_dir, file_end), index_col=0)

    checked_list = []
    faulty_list = []

    for test_df in df_list:
        # create two lists from the correctly formatted csv (accurate) and the data for upload (test)
        accurate = accurate_df.columns.tolist()
        test_cols = test_df.columns.tolist()

        if "DELETE" in test_cols:
            accurate_cols = accurate.copy()
        elif "DELETE_" in test_cols:
            # replace column name and re-create the list of column names
            test_df.rename(columns={'DELETE_': 'DELETE'}, inplace=True)
            test_cols = test_df.columns.tolist()
            accurate_cols = accurate.copy()

        else:
            print("ERROR "*20)
            print("Pipeline shutdown")
            import sys
            sys.exit()

        # check that the columns have the same number of headers as the accurate version.
        if len(accurate_cols) == len(test_cols):
            print('There are the correct amount of columns.')

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

                    print('Error --- column names are incorrect - data will NOT be uploaded.')
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

    return checked_list, faulty_list


def concat_and_clean_df_fn(list_a):
    """ Concatenate open geo-dataframes, drop duplicates, and delete features NOTES and OBJECTID.
      Additionally, if 'Not recorded'  is contained within the LABEL it is removed.

    :param list_a: list object containing open geo-dataframes.
    :return gdf: output geo-dataframe following function processing.
    """
    gdf = pd.concat(list_a)
    # remove duplicates
    gdf.drop_duplicates()
    # drop unwanted columns
    if 'NOTES' in gdf.columns:
        gdf.drop(columns=['NOTES'], inplace=True)

    if 'OBJECTID' in gdf.columns:
        gdf.drop(columns=['OBJECTID'], inplace=True)

    # remove 'Not recorded' from LABEL feature.
    if 'LABEL' in gdf.columns:
        gdf.LABEL.replace("Not recorded", "", inplace=True)

    if 'LABEL' in gdf.columns:
        gdf.LABEL.replace("Not Recorded", "", inplace=True)

    return gdf


def concat_list_fn(list_a, feature_type):
    """

    :param list_a: list object containing open dataframes.
    :param feature_type: string object containing the feature name.
    :return removed_delete_gdf: geo-dataframe
    :return feature_type: string object containing the feature name.
    """

    # Call the concat_and_clean_df function to concatenate open geo-dataframes, drop duplicates, and delete features
    # NOTES and OBJECTID. Additionally, if 'Not recorded'  is contained within the LABEL it is removed.
    input_data_gdf = concat_and_clean_df_fn(list_a)

    print(' - All ', feature_type, ' have been concatenated into one dataframe.')

    if 'DELETE' in input_data_gdf.columns:
        print(' - Concatenated geo-dataframe contains ', len(input_data_gdf.index), ' observations.')
        # delete the variable 1 from the delete column - 2 will be deleted from the for migration pipeline.

        print("Value counts input_data: ", input_data_gdf["DELETE"].value_counts())

        #removed_delete_gdf = input_data_gdf[(input_data_gdf['DELETE'] == 0) | (input_data_gdf['DELETE'] == 2)]
        removed_delete_gdf = input_data_gdf[input_data_gdf['DELETE'].isin([0, 0., "0",  2, 2., "2"])]
        remaining_list = removed_delete_gdf ["DELETE"].unique().tolist()
        
        print(' - Delete observations where DELETE = 1')
        print(' - ', (len(input_data_gdf.index) - len(removed_delete_gdf.index)), ' observations were deleted.')
        print("Value remaining data: ", removed_delete_gdf["DELETE"].value_counts())

        if len(removed_delete_gdf) == 0:
            print("ERROR - Dirty data - check your DELETE column")
            
            import sys
            sys.exit()

    else:
        removed_delete_gdf = input_data_gdf


    if len(removed_delete_gdf.index) > 0:
        pass
    else:
        removed_delete_gdf = None


    return removed_delete_gdf, feature_type


def concat_faulty_list_fn(list_a, feature_type, export_dir, year):
    """ Transition faulty data to the export directory with the a STATUS feature added - variable 'Column Error'.


    :param export_dir: path object to the export directory (commend argument).
    :param list_a: list object containing feature specific faulty geo-dataframes.
    :param feature_type: string object containing the feature name (i.e. lines, paddocks).
   """

    print('!' * 50)
    print('ERROR - ' * 20)
    print('!' * 50)

    print("Len a: ", len(list_a))
    if len(list_a) == 0:
        pass

    else:
        df_concat = pd.concat(list_a)
        df_concat["STATUS"] = "Column Error"

        gdf = gpd.GeoDataFrame(
            df_concat, geometry='geometry')

        for prop in gdf.PROPERTY.unique():
            print('prop: ', prop)
            output = '{0}\\Faulty\\{1}\\{2}_{1}_faulty.shp'.format(export_dir, feature_type.title(), prop.title().replace(" ", "_"))
            print('Faulty geo-dataframe has been transition to: ', output)
        gdf.to_file(output)




def property_export_shapefile_fn(prop_curr_test, pastoral_districts_path, dir_list_item, year):
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

        datetime_object = datetime.strptime(currency, "%Y-%m-%d %H:%M:%S")
        date_str = datetime_object.strftime("%Y%m%d_%H%M%S")

        output_path = os.path.join(pastoral_districts_path, district, property_name, 'Infrastructure', 'Server_Upload',
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


def sort_file_paths_fn(files_list):
    """ Verify all located shapefiles within the upload sub-directory , adding file paths to a delete_files_list that
    contain the feature UPLOAD.

    :param files_list: list object containing all identified file paths within the property upload sub-directory
    within the Pastoral Districts directory (command argument).
    :return delete_files_list: list object containing all files that do not contain an UPLOAD feature within the
    property upload sub-directory. List will be used to delete all file once file transfer has completed.
    """
    delete_files_list = []
    for file in files_list:

        gdf = gpd.read_file(file)
        if 'UPLOAD' not in gdf.columns:
            delete_files_list.append(file)

    return delete_files_list


def main_routine(path, assets_dir, year, export_dir, directory_list):
    """This script

    """
    # print('step1_2_search_folders.py INITIATED.')

    # call the directory_path_fn function to create a path to all property sub-directories and return them as a list.
    prop_list = property_path_fn(path)

    # call the upload_download_path_fn function to create a path to to the Server Upload and Download sub-directories
    # for each property and return them as a list.
    upload_list, download_list = upload_download_path_fn(prop_list)

    # call the extract_paths_fn function to search through each server upload subdirectory within the root pastoral
    # directory directory for line, point and polygon shapefiles, read them in as  geo-dataframe and append them to a
    # list.
    print("Checking that the shapefile geometry is accurate.....")
    points_list, lines_list, poly_list, paddocks_list, files_list = extract_paths_fn(upload_list, year, directory_list)
    delete_files_list = sort_file_paths_fn(files_list)
    print('Files located for processing:')
    for i in delete_files_list:
        print(' - ', i)
    print('=' * 80)

    # ---------------------------------------------- POINTS ------------------------------------------------------------
    print('=' * 50)
    print('POINTS')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    print('Checking column names and lengths....')
    checked_points_list, faulty_points_list = check_column_names_fn(
        points_list, assets_dir, 'Pastoral_Infra_Points_Template.shp')

    print(' - Correct dataframes: ', len(checked_points_list))
    print(' - Incorrect dataframes: ', len(faulty_points_list))

    # --------------------------------------------------- LINES --------------------------------------------------------
    print('-' * 50)
    print('LINES')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    checked_lines_list, faulty_lines_list = check_column_names_fn(
        lines_list, assets_dir, 'Pastoral_Infra_Lines_Template.shp')

    print(' - Correct dataframes: ', len(checked_lines_list))
    print(' - Incorrect dataframes: ', len(faulty_lines_list))


    # ---------------------------------------------------- POLYGONS ----------------------------------------------------

    print('-' * 50)
    print('POLYGONS')

    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    checked_polygons_list, faulty_polygons_list = check_column_names_fn(
        poly_list, assets_dir, 'Pastoral_Infra_Polygons_Template.shp')

    print(' - Correct dataframes: ', len(checked_polygons_list))
    print(' - Incorrect dataframes: ', len(faulty_polygons_list))


    # ---------------------------------------------------- PADDOCKS ----------------------------------------------------

    print('-' * 50)
    print('PADDOCKS')
    # print('!'*50)
    # call the check_column_names_fn function to sort the input dataframe into either the checked_list or faulty_list
    # based on it having only the required column headings.
    checked_paddocks_list, faulty_paddocks_list = check_column_names_fn(
        paddocks_list, assets_dir, 'Pastoral_Infra_Paddocks_Template.shp')

    print(' - Correct dataframes: ', len(checked_paddocks_list))
    print(' - Incorrect dataframes: ', len(faulty_paddocks_list))


    # ------------------------------------------------- WORKFLOW -------------------------------------------------------

    # verify that the checked points file list contains a result
    concat_list = []
    feature_type_list = []

    if len(checked_points_list) > 0:
        print('-'*50)
        print('Processing points.....')
        print('There are ', len(checked_points_list), ' geo-dataframes')
        points_removed_delete_gdf, feature_type = concat_list_fn(
            checked_points_list, 'points')

        concat_list.append(points_removed_delete_gdf)
        feature_type_list.append(feature_type)

    if len(faulty_points_list) > 0:
        concat_faulty_list_fn(faulty_points_list, 'points', export_dir, year)

    if len(checked_lines_list) > 0:
        print('-' * 50)
        print('Processing lines.....')
        print('There are ', len(checked_lines_list), ' geo-dataframes')
        lines_removed_delete_gdf, feature_type = concat_list_fn(
            checked_lines_list, 'lines')

        concat_list.append(lines_removed_delete_gdf)
        feature_type_list.append(feature_type)

    if len(faulty_lines_list) > 0:
        concat_faulty_list_fn(faulty_lines_list, 'lines', export_dir, year)

    if len(checked_polygons_list) > 0:
        print('-' * 50)
        print('Processing polygons.....')
        print('There are ', len(checked_polygons_list), ' geo-dataframes')
        polygons_removed_delete_gdf, feature_type = concat_list_fn(
            checked_polygons_list, 'polygons')
        concat_list.append(polygons_removed_delete_gdf)
        feature_type_list.append(feature_type)

    if len(faulty_polygons_list) > 0:
        concat_faulty_list_fn(faulty_polygons_list, 'polygons', export_dir, year)


    if len(checked_paddocks_list) > 0:
        print('-' * 50)
        print('Processing paddocks.....')
        print('There are ', len(checked_paddocks_list), ' geo-dataframes')
        paddocks_removed_delete_gdf, feature_type = concat_list_fn(
            checked_paddocks_list, 'paddocks')
        concat_list.append(paddocks_removed_delete_gdf)
        feature_type_list.append(feature_type)

    if len(faulty_paddocks_list) > 0:
        concat_faulty_list_fn(faulty_paddocks_list, 'paddocks', export_dir, year)

    return delete_files_list, concat_list, feature_type_list


if __name__ == '__main__':
    main_routine()
