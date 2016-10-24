#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
    \package autoconvert
    \file autoconvert.py
    \author François Bianco, University of Geneva - francois.bianco@unige.ch
    \date 2012.05.22
    \updates
        2016-10: v0.03 Removed options for gwddion image creation. Toby Gill
        2013: v0.02 adding separate Qt Gui and make the script OS independant
                    added a real GNU-like script behaviour to this script
                    with options
        2012: v0.01 command line only
    \mainpage Automatic convert from matrix file to flat file and then images
    \section Copyright
    Copyright (C) 2011 François Bianco, University of Geneva - francois.bianco@unige.ch
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import os
import re
import argparse
import subprocess

debug = False

parser = argparse.ArgumentParser(description='''A script to facilitate
the automatic conversion of STM data with Omicron VernissageCmd and/or
Gwyexport (Gwyddion).''', epilog='''Developped by François Bianco (fbianco) –
francois.bianco@unige.ch © 2013 Under GNU GPL v.3 or above
''')

# Vernissage related options
parser.add_argument('--novernissage', default=False, action='store_true',
    help='Prevent using VernissageCmd to export data.')
parser.add_argument('-vo', '--vernissageoutfolder', default='C:\Users\Tobias Gill\Desktop\\bigblue_flat',
    help='Vernissage export output folder')
parser.add_argument('--vernissagecmd', default='C:\Program Files (x86)\Omicron NanoTechnology\Vernissage\V2.1\Bin\VernissageCmd.exe',
    help='Path/Executable name for VernissageCmd.exe')
parser.add_argument('--vernissageflags',
    default='-path {path} -outdir {outdir} -exporter {exporter}',
    help='''Vernissage command flags, see VernissageCmd.exe for help.
This script use {path} and {outdir} to format the command string.
    ''')
parser.add_argument('--vernissageexporter',
    default='Flattener',
    help='''Can be any Vernissage supported exporter plug-in name.''')

# General options
parser.add_argument('--quiet', dest='verbose', default=True,
    action='store_false',
    help='Output executed command and running informations.')
parser.add_argument('-R', '--recursive', default=False, action='store_true',
    help='Convert also data in all subfolders')
parser.add_argument('--overwrite', default=False, action='store_true',
    help='''Overwrite files if output folders exist. Please double check what
you are doing, since it could results in file overwritten/destroyed.''')

# Input folders containing data
parser.add_argument('inputfolders', nargs='+',
    help='The folder containing the data to convert.')

def build_command_list(cmd, flags='', arguments={}):

    command_list = [cmd,]
    
    argument = re.compile('^\{.*\}$')
    for flag in flags.split(' '):
        if argument.match(flag):
            if isinstance(arguments[flag], list):
                command_list.extend(arguments[flag])
            else:
                command_list.append(arguments[flag])
        else:
            command_list.append(flag)

    return command_list

def convert(dirname, args, stdout=None, stderr=None):
    current_dirpath = os.path.relpath(dirname)

    if not args.novernissage: # Do Vernissage convertion
        vernissageout_dirpath = os.path.join(args.vernissageoutfolder,
                                           current_dirpath)
        if not os.path.isdir(vernissageout_dirpath):
            if args.verbose: print 'Creating %s' % vernissageout_dirpath
            os.mkdir(vernissageout_dirpath)
        elif not args.overwrite: # dir exist + do not overwrite
            return

        # vernissage
        subprocess.call(build_command_list(
                    args.vernissagecmd, args.vernissageflags,
                    {'{path}': current_dirpath,
                     '{outdir}': vernissageout_dirpath,
                     '{exporter}': args.vernissageexporter}),
                     stdout=stdout, stderr=stderr)

def process(inputfolder, args, stdout=None, stderr=None):

    data_dirpath = os.path.abspath(inputfolder)

    if not os.path.isdir(data_dirpath):
        print 'Error %s is not a directory.' % data_dirpath
        return

    if not args.novernissage and not os.path.isdir(args.vernissageoutfolder):
        os.mkdir(args.vernissageoutfolder)

    for dirname, subdirnames, filenames in os.walk(data_dirpath):
        convert(dirname, args, stdout, stderr)

        if args.recursive:
            for subdirname in subdirnames:
                convert(os.path.join(dirname, subdirname), args, stdout, stderr)

def main():
    args = parser.parse_args()
    for inputfolder in args.inputfolders:
        if args.verbose: print 'Converting data in %s' % inputfolder
        process(inputfolder, args)

if __name__ == "__main__":
    main()