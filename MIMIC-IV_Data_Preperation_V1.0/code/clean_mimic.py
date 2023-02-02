import sys
import os
import argparse

import settings
import generate_dictionary
import generate_tuples
import post_process


def main():
    
    parser = argparse.ArgumentParser(description='Whether add label or column, default false.')
    parser.add_argument('--add_label', action='store_true', help ='add labels for the dictionary (need extra data)')
    parser.add_argument('--add_category', action='store_true', help='add categories for the dictionary (need extra data)')    
    args = parser.parse_args()

    # assert paths
    assert os.path.exists(settings.MIMIC_DIR)
    assert os.path.exists(settings.ROLL_UP_SRC)
    assert os.path.exists(settings.UOM_SRC)
    assert os.path.exists(settings.RESULT_ROOT_DIR)
    
    # make directories for output
    if not os.path.exists(settings.RESULT_ROOT_DIR):
        os.mkdir(settings.RESULT_ROOT_DIR)
        
    if not os.path.exists(settings.TUPLE_DIR):
        os.mkdir(settings.TUPLE_DIR)
        
    if not os.path.exists(settings.STRING_TUPLE_DIR):
        os.mkdir(settings.STRING_TUPLE_DIR)
    
    if not os.path.exists(settings.IDX_DIR):
        os.mkdir(settings.IDX_DIR)
    
    # clean MIMIC data
    generate_dictionary.main()
    generate_tuples.main()
    post_process.main(args)
    
    
if __name__=='__main__':
    main()