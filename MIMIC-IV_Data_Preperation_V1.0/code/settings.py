'''
Settings of cleaning
'''

MIMIC_DIR = 'MIMIC-IV_Data_Preperation_V1.0/Raw_MIMIC-IV/'    # save MIMIC-IV v1.0 original finals downloaded from https://physionet.org/content/mimiciv/1.0/
ROLL_UP_SRC = 'MIMIC-IV_Data_Preperation_V1.0/rollup_tables/'   # files of roll-up tables
UOM_SRC  = 'MIMIC-IV_Data_Preperation_V1.0/uom_dependency/'   # files of roll-up tables
RESULT_ROOT_DIR = 'MIMIC-IV_Data_Preperation_V1.0/Cleaned_MIMIC-IV/'    # the directory to output the result

# the following files are under RESULT_ROOT_DIR
TUPLE_DIR = RESULT_ROOT_DIR + 'tuple/'
STRING_TUPLE_DIR = RESULT_ROOT_DIR + 'string_tuple/'
IDX_DIR = RESULT_ROOT_DIR + 'index/'
