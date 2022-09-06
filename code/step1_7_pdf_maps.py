# !/usr/bin/env python

"""
RMB INFRASTRUCTURE TRANSITION PIPELINE
======================================
step1_7_pdf_maps.py
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
import shutil
import warnings
from glob import glob
from datetime import datetime
warnings.filterwarnings("ignore")


def property_path_fn(path):
    """ Create a path to all property sub-directories and return them as a list.

    :param path: string object containing the path to the Pastoral Districts directory.
    :return prop_list: list object containing the path to all property sub-directories.
    """

    # create a list of pastoral districts.
    dir_list = next(os.walk(path))[1]

    prop_list = []
    prop_name_list =[]

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
            prop_name_list.append(prop_name)

    return prop_list, prop_name_list


def pdf_maps_upload_path_fn(prop_list, year):
    """ Create a path to to the Server Upload sub-directory for each property and return them as a list.

    :param prop_list: list object containing the path to all property sub-directories.
    :return upload_list: list object containing the path to each properties Server_Upload sub-directory.
    """
    upload_list = []
    for prop_path in prop_list:
        upload_path = os.path.join(prop_path, 'Infrastructure', 'Server_Upload', str(year), 'pdf_maps')
        upload_list.append(upload_path)

    return upload_list


def main_routine(year, pastoral_districts_path, transition_dir):
    """ Collect all finalised pdf maps from the Pastoral Districts property sub-directories and save them in the for
    migration directory.

    """

    # call the directory_path_fn function to create a path to all property sub-directories and return them as a list.
    prop_list, prop_name_list = property_path_fn(pastoral_districts_path)

    # call the upload_download_path_fn function to create a path to to the Server Upload and Download sub-directories
    # for each property and return them as a list.
    upload_list = pdf_maps_upload_path_fn(prop_list, year)

    output_location = os.path.join(transition_dir, 'For_Migration', 'Pdf_Maps')

    if not os.path.exists(output_location):
        os.mkdir(output_location)

    for upload_map in upload_list:

        if glob("{}\\*.pdf".format(upload_map)):
            for pdf_map in glob("{}\\*.pdf".format(upload_map)):
                print(" - Working on: ", pdf_map)

                file_list = pdf_map.split('\\')
                export_file = "{0}\\{1}".format(output_location, file_list[-1])
                print(export_file)
                print(' - Located: ', file_list[-1])
                if os.path.isfile(export_file):
                    pass

                else:
                    shutil.copy(pdf_map, export_file)
                    print(' -- Copied to FOR MIGRATION directory: ', file_list[-1])

                    path_list, file_ = pdf_map.rsplit('\\', 1)
                    # print('path_list: ', path_list)
                    # print('file_: ', file_)

                    date = datetime.now()
                    string_date = date.strftime("%Y%m%d_%H%M%S")
                    # text_doc = "{0}\\map_transfer.txt".format(path_list)
                    # f = open(text_doc, "w+")
                    # f.write(file_)
                    # f.write(' was moved to the "For Migration" directory for approval at ')
                    # f.write(string_date)
                    # f.write('.\n')
                    # f.close()

            print('Deleting: ')

            # Handle errors while calling os.remove()
            try:
                os.remove(pdf_map)
                print(" - ", pdf_map)
            except:
                print("Error while deleting file ", pdf_map)
        else:
            print(" -- No maps located in ..... ", upload_map)

if __name__ == '__main__':
    main_routine()
