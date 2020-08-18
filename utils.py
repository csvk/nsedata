"""
Created on Mar 10, 2017
@author: Souvik
@Program Function: Misclleneous utility functions


"""

import os, shutil


def mkdir(path):

    if not os.path.exists(path):
        os.makedirs(path)

def rmdir(path):

    if os.path.exists(path):
        shutil.rmtree(path)

def copy_files(src_path, dest_path, files):

    if src_path[-1:] != '/':
        src_path = src_path + '/'
    if dest_path[-1:] != '/':
        dest_path = dest_path + '/'

    print('Initiating copying of {} files'.format(len(files)))

    count = 0
    for file in files:
        if os.path.exists(dest_path + file):
            os.unlink(dest_path + file)
        shutil.copyfile(src_path + file, dest_path + file)
        count += 1

    print('{} files copied'.format(count))


