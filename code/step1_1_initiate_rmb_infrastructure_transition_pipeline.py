# !/usr/bin/env python

"""

### Description ###

The RMB Pastoral Infrastructure Pipeline is the first half of the Pastoral Infrastructure Workflow. 
This Python Pipeline will search through the Darwin Pastoral_Districts directory locate any finalised pdf's and 
shapefiles and run them through collation and general attribute checks for completeness. Data is loaded into a 
"For_Migration" directory for managers observation to assess accuracy and completeness of the data set. 
The script also calculates area and length and a bunch of other things.

RMB INFRASTRUCTURE transition PIPELINE
======================================
step1_1_initiate_rmb_infrastructure_transition_pipeline.py
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
import warnings
from glob import glob
import geopandas as gpd
import sys

warnings.filterwarnings("ignore")


def cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Collate all finalised spatial data and maps into the transition directory.
        transitioned data has been processed area calculated and attributes cleaned.''')

    # p.add_argument('-d', '--directory', type=str, help='The transitory directory.',
    #                default=r'Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT')

    p.add_argument('-x', '--output_dir', type=str, help='Directory path for outputs.',
                   default=r"P:\Pipelines\Output\rmb_infrastructure_transition")

    p.add_argument("-pd", "--pastoral_districts_directory",
                   help="Enter path to the Pastoral_Districts directory in the Spatial/Working drive)",
                   default=r'P:\Pastoral_Districts')

    p.add_argument("-r", "--remote_desktop", help="Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote")
    p.add_argument('-t', '--transition_dir', type=str, help='Directory path for outputs.',
                   default=r"P:\Pastoral_Infrastructure\Transition")

    p.add_argument('-a', '--assets_dir', type=str, help='Directory path containing required shapefile structure.',
                   default=r'E:\DEPWS\code\prod\rangeland_monitoring\rmb_infrastructure_transition_pipeline\assets')

    p.add_argument('-y', '--year', type=str, help='Enter the year (i.e. 2001).')

    cmd_args = p.parse_args()

    if cmd_args.year is None:
        p.print_help()

        sys.exit()

    return cmd_args


def user_id_fn(remote_desktop):
    """ Extract the users id stripping of the adm when on the remote desktop.

    :param remote_desktop: string object containing the computer system being used.
    :return final_user: string object containing the NTG user id. """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    if remote_desktop == 'remote' or 'remote_auto':
        final_user = user[3:]

    else:
        final_user = user

    return final_user


def temporary_dir(final_user):
    """ Create an temporary directory 'user_YYYMMDD_HHMM' in your working directory - this directory will be deleted
    at the completion of the script.

    :param final_user: string object containing the NTG user id.
     """

    # extract the users working directory path.
    home_dir = os.path.expanduser("~")

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    primary_temp_dir = home_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' \
                       + str(date_time_list_split[0]) + str(date_time_list_split[1])

    try:
        shutil.rmtree(primary_temp_dir)

    except:
        pass

    # create folder a temporary folder titled (titled 'tempFolder')
    os.makedirs(primary_temp_dir)

    return primary_temp_dir, final_user


def dir_folders_fn(primary_dir, directory_list):
    """ Create directory tree within the temporary directory based on shapefile type.

    :param primary_dir: string object containing the newly created temporary directory path.
    :param directory_list: list object containing the shapefile types.
    :return direc: string object containing the path to the top of the directory tree (i.e. property name).
    """

    if not os.path.exists(primary_dir):
        os.mkdir(primary_dir)

    for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(primary_dir, i))

        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)


def dir_folders_2_fn(direc, directory_list, feature_type):
    """ Create directory tree within the transitory directory based on shapefile type.

    :param feature_type: string object containing the Shapely object (i.e. lines, points)
    :param year: integer object containing the current year.
    :param direc: string object containing the path to the transitory directory.
    :param directory_list: list object containing the shapefile types.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)

    previous_dir = "{0}\\{1}".format(direc, feature_type)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(previous_dir)

    path_list = []
    for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(previous_dir, i))
        path_list.append(shapefile_dir)
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)


def dir_folders_3_fn(direc, feature_name):
    """ Create directory tree within the transitory directory based on shapefile type.

    :param year: integer object containing the current year.
    :param direc: string object containing the path to the transitory directory.
    :param feature_name: string object containing the name of the sub-directory.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)

    shapefile_dir = ('{0}\\{1}'.format(direc, feature_name))
    if not os.path.exists(shapefile_dir):
        os.mkdir(shapefile_dir)


def migration_dir_folders_fn(direc, directory_list):
    """ Create directory tree within the transitory directory based on shapefile type.

    :param direc: string object containing the path to the transitory directory.
    :param directory_list: list object containing the shapefile types.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)
        # print(direc, ' created.')

    migration_path_list = []
    for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(direc, i))
        migration_path_list.append(shapefile_dir)
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)


def export_file_path_fn(primary_output_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument primary_output_dir.

        :param primary_output_dir: string object containing the path to the export directory (command argument).
        :param final_user: string object containing the NTG user id
        :return output_dir_path: string object containing the newly created directory path for all retained exports. """

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    output_dir_path = primary_output_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(output_dir_path)

    except:
        pass

    # create folder titled 'tempFolder'
    os.makedirs(output_dir_path)

    return output_dir_path


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
    # print('file_path: ', file_path)
    for files in glob(file_path):
        print(search_criteria, 'located.')

    return files


def delete_original_files(delete_files_list):
    """ Delete all original shapefiles and extension files for geo-dataframes with no UPLOAD column.

   :param delete_files_list: string object containing file paths to all files with no UPLOAD column.
   """

    for file in delete_files_list:

        file_path = (file[:-4])
        print('file_path: ', file_path)

        if glob("{0}*".format(file_path)):
            print("Deleting...")
            for file in glob("{0}*".format(file_path)):

                # Handle errors while calling os.remove()
                try:
                    os.remove(file)
                    print(" - ", file)

                except:
                    print("Error while deleting file ", file)

        else:
            print("No files in the were located....")


def copy_original_files(delete_files_list, primary_output_dir):
    """ Delete all original shapefiles and extension files for geo-dataframes with no UPLOAD column.

    :param primary_output_dir:
    :param delete_files_list: string object containing file paths to all files with no UPLOAD column.
    """

    print("Copy original files")
    print("-" * 50)
    for file in delete_files_list:
        print(file)

        if file.endswith(".shp"):
            _, path1, path2, path3 = file.rsplit('\\', 3)
            gdf = gpd.read_file(file, driver="ESRI Shapefile")
            export_path = os.path.join(primary_output_dir, "originals", path3)
            print("export file: ", export_path)
            gdf.to_file(export_path, driver="ESRI Shapefile")


def next_subfolder_fn(path_to_parent):
    try:
        return next(os.walk(path_to_parent))[1]
    except StopIteration:
        return []


def main_routine():
    """ This pipeline hunts through the Pastoral Districts directory for shapefiles located within a properties
    Server_Upload sub-directory. Data undergoes several attribute and spatial checks, if individual property
    """

    """ This script runs the command arguments and controls the pipelines workflow, finally deleting """

    # print('step1_1_initiate_rmb_infrastructure_transition_pipeline.py INITIATED.')

    # read in the command arguments
    cmd_args = cmd_args_fn()
    # direc = cmd_args.directory
    output_dir = cmd_args.output_dir
    pastoral_districts_path = cmd_args.pastoral_districts_directory
    remote_desktop = cmd_args.remote_desktop
    transition_dir = cmd_args.transition_dir
    assets_dir = cmd_args.assets_dir
    year = cmd_args.year
    # migration = cmd_args.migration_directory

    if not year:
        print('You must specify the year the data was collected for (e.g. -y 2021)')
        sys.exit()

    print('=' * 50)
    previous_transfer = os.path.join(transition_dir, "previous_transfer")
    for_migration = os.path.join(transition_dir, "for_migration")
    print('LOCATION OF PREVIOUS transition DIRECTORY: ', previous_transfer)
    print('LOCATION OF MIGRATION DIRECTORY: ', for_migration)
    print('=' * 50)
    print('Collecting all ', year, ' data')

    datetime_object = datetime.now()
    # print(datetime_object)

    date_str = datetime_object.strftime("%Y%m%d_%H%M%S")
    print(date_str)
    

    migration_dirs = next_subfolder_fn(for_migration)
    for dirs in migration_dirs:
        dir_path = os.path.join(for_migration, dirs)
        for files in glob(dir_path + "//*.shp"):
            print(files)
            if files:
                print("ERROR - shapefiles located in the For Migration Directory.")

                sys.exit()

    # todo once sub-dirs have been updated remove second line
    # directory_list = ["Points", "Lines", "Polygons", "Paddocks"]
    directory_list = ["points", "lines", "polygons", "paddocks"]

    pastoral_estate = assets_search_fn("NT_Pastoral_Estate.shp", "assets\\shapefile")

    # call the user_id_fn function to extract the user id
    final_user = user_id_fn(remote_desktop)

    # call the export_file_path_fn function to create an export directory.
    primary_output_dir = export_file_path_fn(output_dir, final_user)
    print("primary_output_dir: ", primary_output_dir)
    # create subdirectories within directories (command arguments)
    dir_folders_3_fn(primary_output_dir, 'originals')
    dir_folders_3_fn(primary_output_dir, 'faulty')
    dir_folders_2_fn(transition_dir, directory_list, 'previous_transfer')
    dir_folders_2_fn(transition_dir, directory_list, 'for_migration')

    # for_migration_path = os.path.join(transition_dir, str(year), "for_migration")


    import step1_2_search_folders
    delete_files_list, concat_list, feature_type_list = step1_2_search_folders.main_routine(
        pastoral_districts_path, assets_dir, year, primary_output_dir, directory_list, date_str, datetime_object)
    # for loop through feature types and feature type specific geo-dataframes

    faulty_gdf_list = []
    #print("concat_list: ", concat_list)
    for gdf, feature_type in zip(concat_list, feature_type_list):
        print("WORKFLOW: ", feature_type)

        # todo tab imports up to line 406
        import step1_3_concatenate_clean
        gdf = step1_3_concatenate_clean.main_routine(
            gdf, feature_type, pastoral_estate)
        
        import step1_4_area_length_currency_date
        gdf, transition_dir, year, feature_type = step1_4_area_length_currency_date.main_routine(gdf, feature_type, transition_dir, year)
        # 
        import step1_5_export_data
        step1_5_export_data.main_routine(
            gdf, feature_type, transition_dir, year, pastoral_districts_path, date_str, datetime_object)

    print('=' * 50)
    print('Original files - NOT PROCESSED have been copied to your TEMPORARY directory.')
    print('They may help you if you need to return DIRTY data......')
    print("Let me know if they are of NO USE to you... You are responsible to delete these if you don't want them.")

    copy_original_files(delete_files_list, primary_output_dir)

    import step1_6_export_faulty_data
    step1_6_export_faulty_data.main_routine(
        year, pastoral_districts_path, primary_output_dir, directory_list, faulty_gdf_list, date_str, datetime_object)

    print('the following files have been upload to the transfer dive and the originals will be deleted:')

    delete_original_files(delete_files_list)

    print('=' * 50)
    print(" - looking for pdf maps....")
    import step1_7_pdf_maps
    step1_7_pdf_maps.main_routine(
        year, pastoral_districts_path, transition_dir)


if __name__ == '__main__':
    main_routine()
