__author__ = 'Tobias Gill'

import os
import BigBlue_dbFunc as bb

def ret_files(path):
    '''
    Returns all flat file names in the directory in path
    '''

    all_files = os.listdir(path)

    for i in range(0, len(all_files)):
        if all_files[i][-4:] != 'flat':
            all_files.remove(all_files[i])

    topo_files = []
    for i in range(0, len(all_files)):
        if all_files[i][-6:] == 'Z_flat':
            topo_files.append(all_files[i])

    spec_files = []
    for i in range(0, len(all_files)):
        if all_files[i][-12:] == 'Aux1(V)_flat' or all_files[i][-12:] == 'Aux2(V)_flat' \
                or all_files[i][-9:] == 'I(V)_flat':
            spec_files.append(all_files[i])

    return all_files, topo_files, spec_files

def add_multiple_files(path, list, username, password):

    for i in range(len(list)):
        temp_data_path = os.path.join(path, list[i])
        tempData = bb.BigBlue(username, password, temp_data_path)
        tempData.add_entry()
        del tempData, temp_data_path