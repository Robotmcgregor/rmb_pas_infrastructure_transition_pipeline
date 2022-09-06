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
from datetime import date
import argparse
import shutil
import warnings
from glob import glob
import sys

warnings.filterwarnings("ignore")


def cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Process raw RMB Mapping result csv -> csv, shapefiles.''')

    p.add_argument('-d', '--directory', type=str, help='The transitory directory.',
                   default=r'Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT')

    p.add_argument('-x', '--export_dir', type=str, help='Directory path for outputs.',
                   default=r"Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\outputs")

    p.add_argument("-pd", "--pastoral_districts_directory",
                   help="Enter path to the Pastoral_Districts directory in the Spatial/Working drive)",
                   default=r'Z:\Scratch\Zonal_Stats_Pipeline\rmb_infrastructure_upload\Pastoral_Districts')

    p.add_argument("-r", "--remote_desktop", help="Working on the remote_desktop? - Enter remote_auto, remote, "
                                                  "local or offline.", default="remote")
    p.add_argument('-t', '--transition_dir', type=str, help='Directory path for outputs.',
                   default=r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT")

    p.add_argument('-a', '--assets_dir', type=str, help='Directory path containing required shapefile structure.',
                   default=r'E:\DENR\code\rangeland_monitoring\rmb_infrastructure_transition_pipeline_v2\assets')

    p.add_argument('-y', '--year', type=str, help='Enter the year (i.e. 2001).',
                   default=date.today().year)

    p.add_argument('-m', '--migration_directory', type=str,
                   help='Enter the path to the infrastructure migration directory.',
                   default=r"U:\Pastoral_Infrastructure_Migration_DO_NOT_EDIT")

    cmd_args = p.parse_args()

    if cmd_args.directory is None:
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
    if remote_desktop == 'remote':
        final_user = user[3:]

    else:
        final_user = user

    return final_user


def temporary_dir(final_user):
    """ Create an temporary directory 'user_YYYMMDD_HHMM' in your working directory - this directory will be deleted
    at the completion of the script.

    :param final_user: string object containing the NTG user id.
    :param export_dir: string object path to an existing directory where an output folder will be created.  """

    # extract the users working directory path.
    home_dir = os.path.expanduser("~")

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    primary_temp_dir = home_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' \
                       + str(date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(primary_temp_dir)

    except:
        pass

    # create folder a temporary folder titled (titled 'tempFolder')
    os.makedirs(primary_temp_dir)

    return primary_temp_dir, final_user


def dir_folders_fn(primary_dir, directory_list):
    """
    Create directory tree within the temporary directory based on shapefile type.

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


def dir_folders_2_fn(direc, directory_list, year, type_):
    """
    Create directory tree within the transitory directory based on shapefile type.

    :param year:
    :param direc: string object containing the path to the transitory directory.
    :param directory_list: list object containing the shapefile types.
    """

    if not os.path.exists(direc):
        os.mkdir(direc)
        print(direc, ' created.')

    year_dir = "{0}\\{1}".format(direc, str(year))
    check_dir = os.path.isdir(year_dir)
    if not check_dir:
        os.mkdir(year_dir)

    """for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(year_dir, i))
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)"""

    previous_dir = "{0}\\{1}".format(year_dir, type_)
    check_dir = os.path.isdir(previous_dir)
    if not check_dir:
        os.mkdir(previous_dir)

    path_list = []
    for i in directory_list:
        shapefile_dir = ('{0}\\{1}'.format(previous_dir, i))
        path_list.append(shapefile_dir)
        if not os.path.exists(shapefile_dir):
            os.mkdir(shapefile_dir)

    return path_list


def migration_dir_folders_fn(direc, directory_list):
    """
    Create directory tree within the transitory directory based on shapefile type.

    :param year:
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


def export_file_path_fn(primary_export_dir, final_user):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument primary_export_dir.

        :param primary_export_dir: string object containing the path to the export directory (command argument).
        :param final_user: string object containing the NTG user id
        :return export_dir_path: string object containing the newly created directory path for all retained exports. """

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = primary_export_dir + '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        pass

    # create folder titled 'tempFolder'
    os.makedirs(export_dir_path)

    return export_dir_path


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
    print('file_path: ', file_path)
    for files in glob(file_path):
        print(search_criteria, 'located.')

    return files


def delete_original_files(delete_files_list):
    """ Delete all original shapefiles and extension files for geo-dataframes with no UPLOAD column.

    :param delete_files_list: string object containing file paths to all files with no UPLOAD column.
    """
    for file in delete_files_list:
        print(file[:-4])
        file_path = (file[:-4])
        for files in glob(file_path + '*'):
            os.remove(files)
            print(' - deleted: ', files)


def main_routine():
    """ This pipeline either downloads the latest ODK Mapping Results csv or searches through a directory defined by
    command argument "remote_desktop". Following the discovery of the ODK Mapping Results csv, the script filters the
    data based on "start_date" and "end_date" command arguments and processes the data to an intermediary file
    defined by the "export_dir" command argument. Once all outputs have been created and filed into separate property
    sub-directories (shapefiles, csv files, downloaded photos, identification request forms), all contents are
    transferred to the relevant property folder in the Pastoral Districts directory in the working drive.

    IMPORTANT - if a point is in another properties Pastoral Estate boundary it will be filed there.

    :param directory: string object (command argument) containing the path to the transitory directory.
    :param export_dir: string object(command argument) containing the path to the desired export location for
    the transitional directory.
    :param remote_desktop: string object (command argument) containing the variable:
    "remote_auto", "remote". Each has its own workflow, remote_auto will download the Results csv from ODK Aggregate
    and process the data, whereas remote or local requires the user to manually download the ODK Mapping Results csv
    and insert it into the "directory_odk" directory before processing.
    :param pastoral_districts_directory: string object (command argument) containing the path to the directory holding
    the Pastoral Estate shapefile.
    """

    print('step1_1_initiate_rmb_infrastructure_transition_pipeline.py INITIATED.')

    # read in the command arguments
    cmd_args = cmd_args_fn()
    direc = cmd_args.directory
    export_dir = cmd_args.export_dir
    pastoral_districts_path = cmd_args.pastoral_districts_directory
    remote_desktop = cmd_args.remote_desktop
    transition_dir = cmd_args.transition_dir
    assets_dir = cmd_args.assets_dir
    year = cmd_args.year
    migration = cmd_args.migration_directory

    print('Collecting all ', year, ' data')

    directory_list = ["points", "lines", "polygons", "paddocks"]

    pastoral_estate = assets_search_fn("NT_Pastoral_Estate.shp", "assets\\shapefile")
    print('Pastoral_estate: ', pastoral_estate)
    # call the user_id_fn function to extract the user id
    final_user = user_id_fn(remote_desktop)

    # call the temporary_dir function to create a temporary folder which will be deleted at the end of the script.
    # primary_temp_dir, final_user = temporary_dir(final_user)

    # create subdirectories within the temporary directory
    # dir_folders_fn(primary_temp_dir, directory_list)

    # call the export_file_path_fn function to create an export directory.
    primary_export_dir = export_file_path_fn(export_dir, final_user)

    status_text_file = "{0}\\{1}".format(primary_export_dir, "status.txt")
    print('status_text_file: ', status_text_file)
    status_text = open(status_text_file, "w+")
    status_text.write('script initiated: \n')
    status_text.write(str(date.today()))
    status_text.write('\n')

    # create subdirectories within the export directory
    # dir_folders_fn(primary_export_dir, directory_list)
    export_dir_list = dir_folders_2_fn(primary_export_dir, directory_list, year, 'outputs')

    previous_transfer_path_list = dir_folders_2_fn(transition_dir, directory_list, year, 'previous_transfer')
    print('previous_transfer_path_list: ', previous_transfer_path_list)

    for_migration_path_list = dir_folders_2_fn(transition_dir, directory_list, year, 'for_migration')
    print('migration_path_list: ', for_migration_path_list)

    for_migration_path = os.path.join(transition_dir, str(year), "for_migration")
    print('for_migration_path: ', for_migration_path)

    list_migration = []
    file_path_list = []
    for i in for_migration_path_list:
        if not os.listdir(i):
            list_migration.extend([0])
            for file in glob("{0}\\{1}".format(i, "*")):
                file_path_list.append(file)
        else:
            list_migration.extend([1])
            print('ERROR !!!!!!')
            print(" -- ", i, "- NOT EMPTY")

    if sum(list_migration) > 0:

        print('You have files in the FOR_MIGRATION directory...')
        print(' - this directory must be empty to run this script.')
        print('-' * 50)
        print('-' * 50)
        print('Do you want me to purge the FOR_MIGRATION directory for you???????')
        value_ = input('Y or N (press enter)')

    else:
        value_ = "Continue"

    if value_ == 'Y':
        print('Purging directory....')
        for s in file_path_list:
            os.remove(s)
            print('- s has been removed..')
        #shutil.rmtree(for_migration_path)
        #print(for_migration_path)
        #print('- directory, sub-directories and all content has been wiped.')

        #del for_migration_path

        for_migration_path_list = dir_folders_2_fn(transition_dir, directory_list, year, 'for_migration')
        print('migration_path_list: ', for_migration_path_list)

    elif value_ == 'N':
        print('Goodbye....')

        import sys
        sys.exit()

    else:
        pass

    if value_ == 'Y' or value_ == 'Continue':
        """raw_transition_path_list = dir_folders_2_fn(transition_dir, directory_list, year, 'raw_transition')
        print('raw_transition_path_list: ', raw_transition_path_list)"""

        # dir_folders_fn('{0}\\faulty'.format(primary_export_dir), directory_list)
        faulty_dir_list = dir_folders_2_fn(primary_export_dir, directory_list, year, 'faulty')
        print('faulty_dir_list: ', faulty_dir_list)

        import step1_2_search_folders
        status_text, delete_files_list, concat_list, feature_type_list = step1_2_search_folders.main_routine(
            pastoral_districts_path, transition_dir,
            assets_dir, year, direc,
            primary_export_dir, migration, assets_dir,
            directory_list, status_text)

        for gdf, feature_type in zip(concat_list, feature_type_list):
            import step1_3_concatinate_clean
            status_text, gdf = step1_3_concatinate_clean.main_routine(
                gdf, feature_type, transition_dir,
                year, status_text, assets_dir, pastoral_estate,
                pastoral_districts_path)

            import step1_4_calc_area_distance
            gdf, transition_dir, year, feature_type, status_text = step1_4_calc_area_distance.main_routine(
                gdf, feature_type, transition_dir,
                year, status_text, assets_dir, pastoral_estate,
                pastoral_districts_path)

            import step1_5_compare_recent_transition_data
            final_test_gdf, final_test_gdf_status, status_text, other_properties_gdf, other_properties_output_status, prop_start_end_dates_gdf, prop_start_end_dates_output_status, original_transition_gdf, original_transition_gdf_status = step1_5_compare_recent_transition_data.main_routine(
                gdf, feature_type, transition_dir,
                year, status_text, assets_dir, pastoral_estate,
                pastoral_districts_path)

            import step1_6_export_data
            step1_6_export_data.main_routine(final_test_gdf, final_test_gdf_status, status_text, other_properties_gdf,
                                             other_properties_output_status, prop_start_end_dates_gdf,
                                             prop_start_end_dates_output_status,
                                             transition_dir, year, assets_dir, pastoral_estate, pastoral_districts_path,
                                             feature_type, original_transition_gdf)

        print('the following files have been upload to the transfer dive and the originals will be deleted:')
        delete_original_files(delete_files_list)

    '''print('for_migration_path_list: ', for_migration_path_list)

    for trans, migration, feature_type in zip(previous_transfer_path_list, for_migration_path_list, directory_list):
        for file in glob("{0}\\{1}".format(trans, "*.shp")):
            print('feature_type:', feature_type)
            gdf = gpd.read_file(file, driver="ESRI Shapefile")

            gdf['DATE_CURR_2'] = pd.to_datetime(gdf['DATE_CURR'], format='%Y-%m-%d')
            #gdf.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT\2021\curr_date.csv")
            print(gdf.info())
            prop_list = gdf.PROPERTY.unique().tolist()

            prop_max_list = []
            for prop_ in prop_list:

                prop_gdf = gdf[gdf['PROPERTY'] == prop_]
                i_list = []
                for i in prop_gdf.DATE_CURR_2:
                    i_list.append(i)
                latest_date_2 = (max(i_list))
                prop_max = prop_gdf[prop_gdf['DATE_CURR_2'] == latest_date_2]
                #prop_max.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\Infrastructure_transition_DO_NOT_EDIT\2021" + '\\' + prop_ + "_curr_date.csv")
                print('prop_max: ', prop_max.info())
                prop_max_list.append(prop_max)

            prop_max_concat_gdf = pd.concat(prop_max_list)

            prop_max_concat_gdf.drop(columns=['DATE_CURR_2'], inplace=True)

            prop_max_concat_gdf.to_file("{0}\\for_migration_{1}_gda94.shp".format(migration, feature_type))
            print('-', feature_type, ' - ', "{0}\\for_migration_{1}_gda94.shp".format(migration, feature_type))

            print(file)'''

    status_text.write('script finished: \n')
    status_text.write(str(date.today()))
    status_text.close()


if __name__ == '__main__':
    main_routine()
