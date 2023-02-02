import sys
import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import json
import rolluptool
from settings import RESULT_ROOT_DIR, TUPLE_DIR, IDX_DIR, MIMIC_DIR, STRING_TUPLE_DIR, UOM_SRC


'''
Generate tuplets from single form
'''


def generate_prescriptions_table(tablename):
    '''
    Generate tuples for Rxnorm (prescriptions.csv).
    roll up: NDC -> Rxnorm
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # roll-up dictionary
    ndc2rxnorm = rolluptool.get_ndc2rxnorm()
    
    # index dictionary
    code2idx = _load_code_dict(tablename)
    
    # load the table
    path = MIMIC_DIR + 'hosp/{}.csv'.format(tablename)
    cols = ['subject_id', 'hadm_id', 'pharmacy_id', 'starttime', 'stoptime', 'drug_type', 'drug', 'gsn', 'ndc', 'prod_strength', 'form_rx', 'dose_val_rx', 'dose_unit_rx', 'form_val_disp', 'form_unit_disp', 'doses_per_24_hrs', 'route']
    
    setting = {'subject_id':'str', 'hadm_id':str, 'ndc':'str', 'starttime':'str'}
    table = pd.read_csv(path, usecols=setting.keys(),
            parse_dates=['starttime'], infer_datetime_format=True,
            dtype='str', index_col=False)
    
    # unify the names and order of columns
    table.rename({'ndc':'code', 'starttime':'time'}, axis=1, inplace=True)
    table = table.loc[:, ['subject_id', 'hadm_id', 'code', 'time']]
    
    # convert all codes to indexes and delete unwanted codes
    table.loc[:, 'code'] = table.loc[:, 'code'].apply(lambda x:ndc2rxnorm.get(x, '<unk>')) 
    table = table.loc[table['code'].isin(code2idx), :]
    table.loc[:, 'code'] = table.loc[:, 'code'].apply(code2idx.get)
    
    # output
    _table2tuples(table, TUPLE_DIR + tablename)


def generate_diagnoses_icd_table(tablename):
    '''
    Generate tuples for diagnoses_icd.csv (PheCode code).
    roll up: ICD -> PheCode
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # roll-up dictionary
    icd102phe = rolluptool.get_icd102phe()
    icd92phe = rolluptool.get_icd92phe()
    
    # index dictionary
    code2idx = _load_code_dict(tablename)
    
    # load the table
    path = MIMIC_DIR + 'hosp/' + tablename + '.csv'
    cols = ['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'icd_version']
    setting = {'subject_id': 'str', 'hadm_id':int, 'icd_code': 'str', 'icd_version':'str'}
    table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    table.rename({'icd_code':'code', 'icd_version':'code_type'}, axis=1, inplace=True)
    print('number of code before rolling up:', 
          table[['code', 'code_type']].drop_duplicates(keep='first', inplace=False).shape[0])
    
    # convert all codes to indexes and delete unwanted codes
    table9 = table.loc[table['code_type'] == '9', :].copy()
    table10 = table.loc[table['code_type'] == '10', :].copy()
    
    table9.loc[~table9['code'].isin(icd92phe), ['code_type']] = 'icd9'
    table9.loc[table9['code'].isin(icd92phe), ['code_type']] = 'phecode'
    table10.loc[~table10['code'].isin(icd102phe), ['code_type']] = 'icd10'
    table10.loc[table10['code'].isin(icd102phe), ['code_type']] = 'phecode'
    
    table9.loc[:, ['code']] = table9.loc[:, 'code'].apply(
        lambda x:(icd92phe[x] if x in icd92phe else x))
    table10.loc[:, ['code']] = table10.loc[:, 'code'].apply(
        lambda x:(icd102phe[x] if x in icd102phe else x))

    table = table9.append(table10)
    del table9
    del table10
    
    table = table.loc[table['code'].isin(code2idx), :]
    table.loc[:, 'code'] = table.loc[:, 'code'].apply(code2idx.get)
    
    # add timestamp for each tuple
    admissions = pd.read_csv(MIMIC_DIR + 'core/admissions.csv', usecols=['hadm_id','dischtime'],
                parse_dates=['dischtime'], infer_datetime_format=True, index_col='hadm_id')
    
    table = table.join(admissions, on=['hadm_id']).loc[:,['subject_id', 'hadm_id', 'code', 'dischtime']]
    print('Time NA:')
    print(table.loc[table['dischtime'].isna()])
    table['hadm_id'] = table['hadm_id'].astype(str)
    table.dropna(inplace=True)

    # output the tuples
    _table2tuples(table, TUPLE_DIR + tablename)


def generate_drgcodes_table(tablename):
    '''
    Generate dictionary for drgcodes.csv (DRG Codes)
    roll up: None
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # index dictionary
    code2idx = _load_code_dict(tablename)
    
    # table
    path = MIMIC_DIR + 'hosp/' + tablename + '.csv'
    cols = ['subject_id', 'hadm_id', 'drg_type', 'drg_code', 'description', 'drg_severity', 'drg_mortality']
    setting = {'subject_id':'str', 'hadm_id':int,'drg_code': 'str'}
    table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    table.rename({'drg_code':'code'}, axis=1, inplace=True)
    print('number of code before rolling up:', 
          table['code'].drop_duplicates(keep='first', inplace=False).shape[0])
    
    # convert all codes to indexes and delete unwanted codes
    table = table.loc[table['code'].isin(code2idx), :]
    table.loc[:, ['code']] = table.loc[:, 'code'].apply(
        lambda x:code2idx[x])
    
    # add timestamp
    admissions = pd.read_csv(MIMIC_DIR + 'core/admissions.csv', usecols=['hadm_id','dischtime'],
                parse_dates=['dischtime'], infer_datetime_format=True, index_col='hadm_id')
    
    table = table.join(admissions, on=['hadm_id']).loc[:,['subject_id', 'hadm_id', 'code', 'dischtime']]
    temp = table.loc[table['dischtime'].isna()]
    if temp.shape[0] != 0:
        print('Time NA:')
        print(temp)
    table['hadm_id'] = table['hadm_id'].astype(str)
    table.dropna(inplace=True)
    
    # output
    _table2tuples(table, TUPLE_DIR + tablename)


def generate_ccs_table(tablename):
    '''
    Generate tuples for hcpcsevents.csv and procedures_icd.csv (CCS Codes)
    roll up: ICD -> CCS
    roll up: CPT -> CCS
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # dictionary
    icd10pcs2css = rolluptool.get_icd10pcs2css()
    icd9cm2ccs = rolluptool.get_icd9cm2ccs()
    cpt2ccs = rolluptool.get_cpt2ccs()
    
    code2idx = _load_code_dict(tablename)
    
    # table ICD
    path = MIMIC_DIR + 'hosp/procedures_icd.csv'
    cols = ['subject_id', 'hadm_id', 'seq_num', 'chartdate', 'icd_code', 'icd_version']
    time = 'chartdate'
    setting = {'subject_id':'str', 'hadm_id':str, 'icd_code': str, 'icd_version':int, time:'str'}
    table = pd.read_csv(path, usecols=setting.keys(), parse_dates=[time],infer_datetime_format=True,
        dtype=setting, index_col=False)
    table.rename({'icd_code':'code', 'icd_version':'code_type', time:'time'},
        axis=1, inplace=True)
    
    # convert all ICD codes to indexes and delete unwanted codes
    table.loc[:, 'code'] = table.loc[:, ['code', 'code_type']].apply(
        lambda x:icd9cm2ccs.get(x[0], '<unk>') if x[1] == 9 else icd10pcs2css.get(x[0], '<unk>'), axis=1)
    table = table.loc[table['code'].isin(code2idx), ['subject_id', 'hadm_id', 'code', 'time']]
    table.loc[:, 'code'] = table.loc[:, 'code'].apply(
        lambda x:code2idx[x])
    
    # table CPT
    path = MIMIC_DIR + 'hosp/hcpcsevents.csv'
    cols = ['subject_id', 'hadm_id', 'chartdate', 'hcpcs_cd', 'seq_num', 'short_description']
    time = 'chartdate'
    setting = {'subject_id':'str', 'hadm_id':str, 'hcpcs_cd': 'str', time:'str'}
    table1 = pd.read_csv(path, usecols=setting.keys(), parse_dates=[time],infer_datetime_format=True,
        dtype=setting, index_col=False)
    table1.rename({'hcpcs_cd':'code', time:'time'}, axis=1, inplace=True)
    
    # convert all CPT codes to indexes and delete unwanted codes
    table1.loc[:, 'code'] = table1.loc[:, 'code'].apply(
        lambda x:cpt2ccs.get(x, '<unk>'))
    table1 = table1.loc[table1['code'].isin(code2idx), ['subject_id', 'hadm_id', 'code', 'time']]
    table1.loc[:, 'code'] = table1.loc[:, 'code'].apply(
        lambda x:code2idx[x])
    
    # concatenate ICD and CPT table together
    table = pd.concat((table, table1))
    del table1
    
    # output
    _table2tuples(table, TUPLE_DIR + tablename)


def generate_no_value_table(tablename):
    '''
    Generate tuples for procedureevents.csv and inputevents.csv respectively
    
    Parameters:
    ----
        tablename:
            Indicate the name of table (procedureevents/inputevents)
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # index dictionary
    code2idx = _load_code_dict(tablename)
    
    # load table
    path = MIMIC_DIR + 'icu/{}.csv'.format(tablename)
    setting = {'subject_id':str, 'hadm_id':str, 'itemid':'str'}
    table = pd.read_csv(path, usecols=['subject_id', 'hadm_id', 'itemid', 'starttime'], parse_dates=['starttime'],
            dtype=setting, index_col=False)
    
    table = table.loc[table['itemid'].isin(code2idx), :]
    table.loc[:, 'itemid'] = table.loc[:, 'itemid'].apply(code2idx.get)
    
    table = table.loc[:,['subject_id', 'hadm_id', 'itemid', 'starttime']]

    _table2tuples(table, TUPLE_DIR + tablename)


def generate_output_table(tablename='outputevents'):
    '''
    Generate tuples for outputevents
    
    Parameters:
    ----
        tablename:
            Indicate the name of table outputevents
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # index dictionary
    dic = pd.read_csv(IDX_DIR + 'code_dict.csv', dtype='str', index_col=0)
    dic = dic.loc[dic['source_table'] == tablename, ['code', 'code_type', 'with_value']]
    code2idx = {i.code:i.code_type + '_' + i.code for i in dic.itertuples(False)}
    print('code dict size:', len(dic))

    code_with_value = set(dic.loc[dic['with_value'] == '1', 'code'])
    
    # patients dictionary
    origin_patients = _load_patients()

    # a dictionary to normalize units
    with open(UOM_SRC + '{}_uom_dict.json'.format(tablename), 'r', encoding='utf8') as f:
        uom_dict = json.load(f)
        uom_dict = {k:v for k,v in uom_dict.items()}
    
    # load the source table
    src_path = MIMIC_DIR + '{}/{}.csv'.format('icu', tablename)
    setting = {'subject_id':str, 'hadm_id':str, 'charttime':None, 'itemid':str, 'value':str, 'valueuom':str}
    with pd.read_csv(src_path, usecols=setting.keys(), index_col=False, parse_dates=['charttime'],
            chunksize=30000000, dtype=setting) as reader:
        for i, chunk in enumerate(reader):
            patients = {i:[] for i in  origin_patients}
            chunk = chunk.loc[:, ['subject_id', 'hadm_id', 'charttime', 'itemid', 'value', 'valueuom']]
            
            for pid, hadm, time, itemid, value, valueuom in tqdm(chunk.itertuples(False), total=chunk.shape[0]):     
                
                # Filter unwanted codes
                if itemid not in code2idx:
                    continue

                # normalize unit of measurement
                unit = _normalize_unit(valueuom)
                
                # create a tuple
                tuple = [hadm, str(time), code2idx[itemid], '']
                
                if itemid in code_with_value and not pd.isna(value):
                    tuple[3] = value
                
                patients[pid].append(tuple)
            
            # output tuples
            _value_table2tuples(patients, TUPLE_DIR + tablename + str(i))


def generate_transfers_table(tablename='transfers'):
    '''
    Generate tuples for transfers.csv
    
    Parameters:
    ----
        tablename:
            Indicate the name of table transfers
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    # index dictionary
    code2idx = _load_code_dict(tablename)

    # patients dictionary
    origin_patients = _load_patients()
    
    # load the source table
    src_path = MIMIC_DIR + '{}/{}.csv'.format('core', tablename)
    setting = {'subject_id':str, 'hadm_id':str, 'intime':None, 'eventtype':str, 'careunit':str}
    with pd.read_csv(src_path, usecols=setting.keys(), index_col=False, parse_dates=['intime'],
            chunksize=30000000, dtype=setting) as reader:
        for i, chunk in enumerate(reader):
            patients = {i:[] for i in  origin_patients}
            chunk = chunk.loc[:, ['subject_id', 'hadm_id', 'intime', 'eventtype', 'careunit']]
            
            for pid, hadm, time, itemid, care_unit in tqdm(chunk.itertuples(False), total=chunk.shape[0]):     
                
                # create a tuple
                tuple = ['', str(time), code2idx[itemid], '']
                
                if not pd.isna(hadm):
                    tuple[0] = hadm
                    
                if not pd.isna(care_unit):
                    tuple[3] = care_unit
                
                patients[pid].append(tuple)
            
            # output tuples
            _value_table2tuples(patients, TUPLE_DIR + tablename + str(i))
                    

def generate_value_table(tablename='labevents', filedir='icu', value_col='valuenum'):
    '''
    Generate tuples for labevents and chartevents respectively.
    
    Parameters:
    ----
        tablename:
            Indicate the name of table (labevents/chartevents)
        filedir:
            Indicate the directory of table (hosp/icu)
        value_col:
            The column containing value of code (value/valuenum)
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating tuples of', tablename)
    
    assert (tablename in ['labevents', 'chartevents'])
    assert (filedir in ['hosp', 'icu'])
    assert (value_col in ['value', 'valuenum'])
    
    # index dictionary
    dic = pd.read_csv(IDX_DIR + 'code_dict.csv', dtype='str', index_col=0)
    dic = dic.loc[dic['source_table'] == tablename, ['code', 'code_type', 'with_value']]
    code2idx = {i.code:i.code_type + '_' + i.code for i in dic.itertuples(False)}
    print('code dict size:', len(dic))

    code_with_value = set(dic.loc[dic['with_value'] == '1', 'code'])
    
    # patients dictionary
    origin_patients = _load_patients()

    # a dictionary to normalize units
    with open(UOM_SRC + '{}_uom_dict.json'.format(tablename), 'r', encoding='utf8') as f:
        uom_dict = json.load(f)
        uom_dict = {k:v for k,v in uom_dict.items()}
    
    # load the source table
    src_path = MIMIC_DIR + '{}/{}.csv'.format(filedir, tablename)
    setting = {'subject_id':str, 'hadm_id':str, 'charttime':None, 'itemid':str, 'value':str,
               value_col:float, 'valueuom':str}
    if value_col == 'valuenum':
        setting['value'] = str
        
    with pd.read_csv(src_path, usecols=setting.keys(), index_col=False, parse_dates=['charttime'],
            chunksize=20000000, dtype=setting) as reader:
        for i, chunk in enumerate(reader):
            patients = {i:[] for i in  origin_patients}
            patients_str = {i:[] for i in  origin_patients}
            
            chunk = chunk.loc[:, ['subject_id', 'hadm_id', 'charttime', 'itemid', 'value', value_col, 'valueuom']]
            for pid, hadm, time, itemid, value, valuenum, valueuom in tqdm(chunk.itertuples(False), total=chunk.shape[0]):     
                
                # Filter unwanted codes
                if itemid not in code2idx:
                    continue

                # normalize unit of measurement
                unit = _normalize_unit(valueuom)
                
                # create a tuple: [admission_id, time, code, value]
                tuple = ['', str(time), code2idx[itemid], '']
                tuple_str = ['', str(time), code2idx[itemid], '']
                
                if not pd.isna(hadm):
                    tuple[0] = hadm
                    tuple_str[0] = hadm
                
                if itemid in code_with_value:   # code with value
                        if pd.isna(valuenum): # the code value is empty
                            if pd.isna(value) or value.strip() == '':
                                tuple[3] = '_MISSING'
                            else:
                                tuple[3] = '_STRING'
                                tuple_str[3] = value
                        elif valuenum == 0:
                            tuple[3] = '0'
                        elif uom_dict[itemid]['<main>'] != unit: # the uom here is not consistent with the main uom of code
                            # code with value and appropriate unit of measurement
                            if unit in uom_dict[itemid] and uom_dict[itemid][unit] != 0:
                                tuple[3] = str(valuenum*uom_dict[itemid][unit])
                            else:   # the code value exists, but it is not valid
                                tuple[3] = '_STRING'
                                tuple_str[3] = value + '#' + unit
                        else:   # the uom here is consistent with the main uom of code
                            tuple[3] = str(valuenum)
                
                else:   # code without value
                    if pd.isna(value) or value.strip() == '': # the code value is empty
                        tuple[3] = '_EMPTY'
                    else: # the code value is not empty
                        tuple[3] = '_STRING'
                        tuple_str[3] = value

                
                # add the item to patients' record
                patients[pid].append(tuple)
                
                # add the string item to patients' record
                if tuple[3] == '_STRING':
                    patients_str[pid].append(tuple_str)

            # output tuples
            _value_table2tuples(patients, TUPLE_DIR + tablename+str(i))
            _value_table2tuples(patients_str, STRING_TUPLE_DIR + '{}{}{}'.format(tablename, '_string_', i))


def merge_tuples(src_dir, cols, out_path):
    '''
    Merge tuples of all tables together.
    
    Parameters:
    ----
        src_dir: source directory of tuples
        cols: column names of output file
        out_path: filepath to output the merged tuples
            
    Returns:
    ----
        No return
    '''
    
    print("\nMerging tuples in {}".format(src_dir))
    
    tuples_out = open(out_path, 'w', encoding='utf8')
    tuples_out.write(','.join(cols) + '\n')
    
    iFiles = os.listdir(src_dir)
    iFiles = [open(src_dir + i, 'r', encoding='utf8') for i in iFiles if '.tri' in i]
    
    while True:
        p = []
        data = []
        for f in iFiles:
            pp, dd = _get_patient_data(f, 10000)
            
            if len(pp) == 0:
                print('Merging finished.')
                return
                
            p.append(pp)
            data.append(dd)
        
        for i in range(1, len(p)):
            if not (p[i] == p[i-1]):
                print('error!')
                exit(1)
                
        for i, p_id in enumerate(p[0]):
            temp = []
            
            for slice in data:
                temp += slice[i]
            
            # if the patient has no tuple, then ignore him/her
            if len(temp) == 0:
                continue
            
            temp.sort(key=lambda x:x[1])
            
            for l in temp:
                tuples_out.write(p_id + ',' + ','.join(l) + '\n')
    


# functions for data IO
def _load_code_dict(tablename):
    '''
    load the dictionary.
    '''
    
    dic = pd.read_csv(IDX_DIR + 'code_dict.csv', dtype='str', index_col=0)
    dic = dic.loc[dic['source_table'] == tablename, ['code', 'code_type', 'with_value']]
    code2idx = {i.code:i.code_type + '_' + i.code for i in dic.itertuples(False)}
    print('code dict size:', len(dic))
    return code2idx


def _table2tuples(table, oFile):
    '''
    Convert a pandas.Dataframe table to a batch of tuples and output
    
    Parameters:
    ----
        table:
            The table to output as tuple
        oFile:
            file path of the output file
            
    Returns:
    ----
        No return
    '''
    
    # load all patients
    patients = _load_patients()

    for p, v, c, t in tqdm(table.itertuples(False), total=table.shape[0]):
        patients[p].append((v, str(t), c, ''))
    
    with open(oFile + '.tri', 'w', encoding='utf8') as f:
        for id, info in patients.items():
            f.write(str(id) + '\n')
            for l in info:
                f.write(','.join(l) + '\n')
            f.write('\n')


def _value_table2tuples(patients, oFile):
    '''
    Output tuples for a table containing code with value.
    
    Parameters:
    ----
        patients:
            tuples to output

        oFile:
            file path of the output file
            
    Returns:
    ----
        No return
    '''
    
    with open(oFile + ".tri", 'w', encoding='utf8') as f:
        for id, info in patients.items():
            f.write(id + '\n')
            for l in info:
                l[3] = l[3].replace(',', '/')
                f.write(','.join(l) + '\n')
            f.write('\n')


def _load_patients():
    '''
    load all patients' ID.
    '''
    
    patients = pd.read_csv(MIMIC_DIR + 'core/patients.csv', usecols=['subject_id'], dtype='str')
    patients = {i:[] for i in patients['subject_id']}
    return patients


def _normalize_unit(unit):
    '''
    normalize unit of measurement
    '''
    
    if pd.isna(unit):
        return 'nan'
    else:
        unit = unit.lower().strip()
        if unit == '' or unit == 'none' or unit == 'nan':
            return 'nan'
        else:
            return unit


def _get_patient_data(f, batch_size):
    '''
    Read a batch of patients' ID and corresponding tuples
    '''
    
    p = []
    d = []
    
    for i in range(batch_size):
        patient = f.readline()[:-1]
        if patient == '':
            break
        
        p.append(patient)
        data = []
        
        while True:
            line = f.readline()[:-1]
            if line == '':
                break
        
            line = line.strip().split(',')
            data.append(line)

        d.append(data)
        
    return p, d


def main():
    # generate a contemporary tuple file for each table
    generate_prescriptions_table('prescriptions')
    generate_ccs_table('ccs')
    generate_diagnoses_icd_table('diagnoses_icd')
    generate_drgcodes_table('drgcodes')
    generate_transfers_table('transfers')
    
    generate_no_value_table('procedureevents')
    generate_no_value_table('inputevents')

    generate_output_table('outputevents')
    generate_value_table('labevents', 'hosp', 'valuenum')
    generate_value_table('chartevents', 'icu', 'valuenum')
    
    # merge all the tuple files together
    cols = ['patient_id', 'admission_id', 'time', 'code', 'value']
    merge_tuples(TUPLE_DIR, cols, RESULT_ROOT_DIR + 'tuples.csv')
    merge_tuples(STRING_TUPLE_DIR, cols, RESULT_ROOT_DIR + 'string_tuples.csv')


if __name__=='__main__':
    main()

    