#!/usr/bin/python
# dupe_checker.py -- A simple script for parsing an iTunes library and
# sorting the files into one of these categories:
#   - File exists in library XML and exists in file system
#   - File exists in library XML but missing from file system
#   - File NOT in library XML, but exists in file system
#         - May be a duplicate
#         - May be unique
#
# Copyright (C) 2017
#
# This software may be modified and distributed under the terms
# of the MIT license.  See the LICENSE file for details.

import argparse
import json
import os
import subprocess
import pickle
import sys
import ntpath
from threading import Lock
from multiprocessing.pool import ThreadPool as Pool
import xml.etree.ElementTree as ET
import urllib.request

checksums_dict = {}
checksums_dict_lock = Lock()
file_path_dict = {}
file_path_dict_lock = Lock()

pool_size = 5  # your "parallelness"
pool = Pool(pool_size)


class FileAttributes:
    def __init__(self):
        self.file_path=None
        self.file_name=None
        self.checksum=None
        self.itunes_key=-1
        self.itunes_file_path=None


def path_leaf(path):
    ''' Taken from https://stackoverflow.com/questions/8384737/extract-file-name-from-path-no-matter-what-the-os-path-format'''
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def checksum_file(filename):
    result = subprocess.check_output(['cksum', filename])
    checksum = result.split(' ')[0]
    return checksum


def process_file(file_name):
    print( "<<<" + file_name + ">>>" )
    file_cksum = checksum_file(file_name)
    new_file = FileAttributes()
    new_file.file_path = file_name
    new_file.file_name = path_leaf(file_name)
    new_file.checksum = file_cksum
    file_path_dict_lock.acquire()
    try:
        file_path_dict[file_name] = new_file
    finally:
        file_path_dict_lock.release()


def calculate_all_checksums(walk_dir):
    print( '--\n--Calculating File Checksums (This will take a LOOOOONG time)\n--')
    for root, subdirs, files in os.walk(walk_dir):
        for file in files:
            pool.apply_async(process_file, (os.path.join(root, file),))
    pool.close()
    pool.join()

def read_itunes_library(xml_file):
    try:
        xml_tree = ET.parse(xml_file)
        xml_root = xml_tree.getroot()

        cur_key = -1

        for this_node in xml_root.findall("./dict/dict/dict"):
            for data in this_node.iter():
                if data.tag == 'key':
                    cur_key = data.text
                elif data.tag == 'dict':
                    this_dict = {}
                    sub_key = ''
                    for attribute in data.iter():
                        if attribute.tag == 'key':
                            sub_key = attribute.text
                        else:
                            this_dict[sub_key] = attribute.text
                        # print 'data6\t\t\t\t', sub_key, attribute.tag, attribute.text, attribute.attrib
                    try:
                        unquoted_path = urllib.request.unquote(this_dict['Location']).replace('file://','')
                        print( this_dict['Name'], this_dict['Location'], unquoted_path )
                        file_path_dict_lock.acquire()
                        try:
                            if unquoted_path not in file_path_dict:
                                print( 'WARNING: ' + unquoted_path + ' was in iTunes Library, but not in file system' )
                                file_path_dict[unquoted_path] = FileAttributes()
                            file_path_dict[unquoted_path].itunes_file_path = unquoted_path
                            file_path_dict[unquoted_path].itunes_key = cur_key
                        except Exception as detail:
                            print( 'Exception: ' + str(detail) )
                        finally:
                            file_path_dict_lock.release()
                    except Exception as detail:
                        print( 'Exception: ' + str(detail) )

    except Exception as detail:
        print( 'Exception: ' + str(detail) )
        return

def generate_reports(report_path):
    if not os.path.exists(report_path):
        os.mkdir(report_path)
    create_dupes_map()

    fh_not_in_itunes_db         = open(report_path + 'not_in_itunes.csv', 'w')
    fh_missing_file             = open(report_path + 'not_in_itunes.csv', 'w')
    fh_file_in_itunes_db        = open(report_path + 'file_in_itunes.csv', 'w')
    fh_dupe_file_orig_in_itunes = open(report_path + 'dupe_file_orig_in_itunes.csv', 'w')

    file_path_dict_lock.acquire()
    temp = None
    try:
        for file_name, file_data in file_path_dict.items():
            temp = file_data
            if file_data.itunes_file_path is None:
                fh_not_in_itunes_db.write('"' + file_data.file_name + '","' + file_data.file_path + '"\n')
            elif file_data.file_path is None:
                fh_missing_file.write('"' + path_leaf(file_data.itunes_file_path) + '","' + file_data.itunes_file_path + '"\n')
            else:
                fh_file_in_itunes_db.write('"' + file_data.file_name + '","' + file_data.file_path + '"\n')
    except Exception as details:
        print( 'Exception: ' + details.message )
    finally:
        file_path_dict_lock.release()


    # Search through duplicate files and create list of those that have a copy in iTunes
    try:
        for checksum, file_data_array in checksums_dict.items():
            temp = file_data_array
            # If there are multiple files with the same checksum
            if file_data_array.len() > 1:
                file_in_itunes = None
                # Try to find the file in iTunes
                for cur_file in file_data_array:
                    if cur_file.itunes_file_path is not None:
                        if file_in_itunes is not None:
                            print( 'Duplicate files found in the iTunes library: ' + file_in_itunes.file_name + " and " + cur_file.itunes_file_path )
                        file_in_itunes = cur_file
                for cur_file in file_data_array:
                    if file_in_itunes is None:
                        print( cur_file.file_path + ' is a duplicate, but does not have a copy in iTunes DB' )
                    elif file_in_itunes is not cur_file:
                        fh_dupe_file_orig_in_itunes.write('"' + cur_file.file_name + '","' + cur_file.file_path + '"')
    except:
        print( 'Exception while looking for duplicate files: ' + details.message )

def create_dupes_map():
    file_path_dict_lock.acquire()
    try:
        for file_name, file_data in file_path_dict.items():
            if file_data.checksum is not None:
                if not file_data.checksum in checksums_dict:
                    checksums_dict[file_data.checksum] = []
                checksums_dict[file_data.checksum].append(file_data)
    except Exception as details:
        print( 'Exception: ' + details.message )
    finally:
        file_path_dict_lock.release()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Search iTunes XML database and file system for duplicate/missing files')
    parser.add_argument('--xml', dest='xml_file', default='',
                        help='Exported iTunes Database in XML format (File->Library->Export Library...)')
    parser.add_argument('--dir', dest='itunes_dir', default='',
                        help='Base directory of the iTunes library on the file system')
    parser.add_argument('--cache', dest='cache_file', default='',
                        help='Optional cache file to load the internal data of this tool (for debugging use, when you' +
                             'don\'t want to re-parse all of the data')
    try:
        # allow command line arguments to be commented out by starting with #
        good_args = []
        for arg in sys.argv[1:]:
            if (not arg.startswith('#')) and (not arg.startswith('--#')) and (not arg.startswith('-#')):
                good_args.append(arg)

        args = parser.parse_args(good_args)
        walk_dir = args.itunes_dir
        xml_file = args.xml_file
        cache_file = args.cache_file
    except Exception as detail:
        print( 'Exception: ' + str(detail) )
        parser.print_help()

    if walk_dir == '':
        #walk_dir = '/Volumes/Raid/iTunes'
        walk_dir = '/Volumes/Raid/iTunes/iTunes Music/Music/Wynonna Judd/Wynonna/'

    if xml_file == '':
        xml_file = '/Volumes/Raid/Library copy.xml'

    print( 'iTunes Library root directory: ' + walk_dir )
    print( 'XML Library file: ' + xml_file )
    if cache_file == '':
        calculate_all_checksums(walk_dir)
        read_itunes_library(xml_file)
        try:
            cache_file = xml_file.replace('.xml', '.pkl').replace(' ', '_')
            print( 'Saving cached data to ' + cache_file )
            fh = open(cache_file, 'wb')
            pickle.dump(file_path_dict, fh)
            fh.close()
        except Exception as detail:
            print( 'Exception: ' + str(detail) )
    else:
        try:
            print( 'Loading cached data from: ' + cache_file )
            cache_fh = open(cache_file,'rb')
            file_path_dict = pickle.load(cache_fh)
            cache_fh.close()
        except Exception as detail:
            print( 'Exception while loading cached data from ' + cache_file + ' ' + str(detail) )
            exit(-1)

    report_directory = './output_'+os.path.basename(xml_file).replace('.','_').replace(' ','_') + '/'

    generate_reports(report_directory)




    # file_name = '/Volumes/Raid/iTunes/iTunes Music Library.xml'

