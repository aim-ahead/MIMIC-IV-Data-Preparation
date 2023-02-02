import sys
import os
import numpy as np
import pandas as pd
import json
from tqdm import tqdm
from settings import RESULT_ROOT_DIR, TUPLE_DIR, IDX_DIR, MIMIC_DIR


def generate_patient_dict(tuple_path, out_path):
    '''
    Generate a patients' dictionary which contains 
    personal information of each patient.
    
    Parameters:
    ----
        out_path:
            filepath to output the dictionary
            
    Returns:
    ----
        None
    '''
    
    # load core/patients.csv
    patients = pd.read_csv(MIMIC_DIR + 'core/patients.csv', dtype={'subject_id':'str'}, index_col=False)
    print('patients', patients.shape)
    
    # load hosp/admission.csv
    admissions = pd.read_csv(MIMIC_DIR + 'core/admissions.csv', dtype={'subject_id':'str'},
                parse_dates=['admittime', 'dischtime'], infer_datetime_format=True, index_col=False)
    
    # add patients' first check-in time and last check-out time
    admissions.sort_values(['admittime'], ascending=True, inplace=True)
    first_info = admissions.drop_duplicates(['subject_id'], keep='first', inplace=False)
    admissions.sort_values(['dischtime'], ascending=False, inplace=True)
    last_info = admissions.drop_duplicates(['subject_id'], keep='first', inplace=False)
    
    first_info = first_info.loc[:,['subject_id', 'admittime', 'ethnicity','marital_status','language']]
    last_info = last_info.loc[:,['subject_id', 'dischtime', 'deathtime']]
    first_info.set_index(['subject_id'], inplace=True)
    last_info.set_index(['subject_id'], inplace=True)
    print('first_last_info', first_info.shape, last_info.shape)
    
    patients = patients.join(first_info, ['subject_id'])
    patients = patients.join(last_info, ['subject_id'])
    print('all_patient_info', patients.shape)

    patients.rename({'deathtime':'death_time', 'admittime':'in_time', 
                     'dischtime':'out_time', 'anchor_age':'age'}, axis=1, inplace=True)
    
    
    # eliminate patients whose check-in time is greater than check-out time
    """
        Warning: This operation will change the frequency of some codes in code_dict.csv, 
                 so you should re-count all the frequency if you uncomment the line below.
        
        patients = patients.loc[~(patients['in_time'] > patients['out_time']), :]
    """
    
    # eliminate patients whose health record is void
    recorded_patients = _get_patients_with_records(tuple_path)
    patients = patients.loc[patients['subject_id'].isin(recorded_patients), :]
    
    patients = patients.loc[:, ['subject_id','gender','age','ethnicity','marital_status', 
                  'language','in_time','out_time','death_time']]
    
    # add icu stay info of patients
    patients = _add_icu_info(patients)

    print('patients_dict.csv shape', patients.shape)
    patients.to_csv(out_path, index=False)


def _get_patients_with_records(tuple_path):
    '''
    Find Patients with records in tuples.csv
    and return IDs of these patients.
    
    Parameters:
    ----
        tuple_path:
            filepath of tuples.txt
            
    Returns:
    ----
        IDs of Patients with records
    '''
    
    print('===================================')
    print('Find patients with health record (tuples).')
    
    patients = set()
    
    with pd.read_csv(tuple_path, index_col=False, usecols=[0],
            chunksize=30000000, dtype='str') as reader:
        for i, chunk in enumerate(reader):
            for pid in tqdm(chunk.itertuples(False), total=chunk.shape[0]):     
                patients.add(pid[0])
        
    
    print('total patients', len(patients))
    print('===================================')

    return patients


def revise_code_dict(input_dict_path, tuple_path, output_dict_path, add_label=False):
    '''
    Revise the frequencies in dictionary according to
    the tuples.txt (namely re-count the frequencies of all codes).
    
    Parameters:
    ----
        input_dict_path:
            filepath of original dictionary
        tuple_path:
            filepath of tuples.txt
        output_dict_path:
            filepath of updated dictionary
        add_label:
            whether add labels to the codes (need extra source files)
            
    Returns:
    ----
        None
    '''
    
    print("Revising the dictionary...")
    
    # load the original dictionary
    original_dict = pd.read_csv(input_dict_path, 
        usecols=['code', 'code_type', 'value_frequency', 'total_frequency'], index_col=False)

    original_dict = {i[1]+'_'+i[0]:(i[2], i[3]) for i in original_dict.itertuples(False)}
    value_freq_dict = {i:0 for i in original_dict}
    total_freq_dict = {i:0 for i in original_dict}

    # count the frequency of codes
    with pd.read_csv(tuple_path, index_col=False,
            chunksize=30000000, dtype='str') as reader:
        for i, chunk in enumerate(reader):
            for pid, hadm, time, code, value in tqdm(chunk.itertuples(False), total=chunk.shape[0]):     
    
                total_freq_dict[code] += 1
                
                def isFloatNum(str):
                    try:
                        float(str)
                    except:
                        return False
                    else:
                        return True

                if not pd.isna(value) and isFloatNum(value):
                    value_freq_dict[code] += 1


    # print updated codes
    print('checking freq...')
    for k,v in original_dict.items():
        if v[0] != value_freq_dict[k]:
            print('[Value freq changed] index:', k, ' freq:', v[0], '->', value_freq_dict[k])
        if v[1] != total_freq_dict[k]:
            print('[Total freq changed] index:', k, ' freq:', v[1], '->', total_freq_dict[k])
        
    new_dict = pd.read_csv(input_dict_path, index_col=False)

    new_dict['value_frequency'] = new_dict[['code_type', 'code']].apply(lambda x:value_freq_dict[x[0]+'_'+x[1]], axis=1)
    new_dict['total_frequency'] = new_dict[['code_type', 'code']].apply(lambda x:total_freq_dict[x[0]+'_'+x[1]], axis=1)

    # add labels to the codes in new dictionary
    if add_label:
        _add_label(new_dict)
    
    # output dictionary
    new_dict.to_csv(output_dict_path, index=False)


def _add_label(dic):
    
    label_dict, desc_dict = _get_label_dict(set(dic['code']))
    
    # drg extra
    drg_dict = {}
    
    path = MIMIC_DIR + 'hosp/' + 'drgcodes.csv'
    cols = ['subject_id', 'hadm_id', 'drg_type', 'drg_code', 'description', 'drg_severity', 'drg_mortality']
    setting = {'drg_code': 'str', 'description':'str'}
    drg_table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    
    for line in drg_table[['drg_code', 'description']].itertuples(False):
        drg_dict[line[0]] = line[1]
    
    
    # map
    label = []
    for line in dic[['code','code_type']].itertuples(False):
        if line[0] not in label_dict[line[1]]:
            if line[1] == 'drg':
                label.append(drg_dict[line[0]])
                # print(line[0], drg_dict[line[0]])
            elif line[1] == 'transfer':
                label.append('')
            else:
                print(line, 'not found')
                label.append('')
        else:
            label.append(label_dict[line[1]][line[0]])
        
    desc = []
    for line in dic[['code','code_type']].itertuples(False):
        desc.append(desc_dict[line[1]].get(line[0], ''))
    
    dic['label'] = label
    dic['description'] = desc
    
    return dic
    

def _get_label_dict(code_set:set):
    # rxnorm, icd10, icd9, phecode, drg, ccs, and mimic
    
    label_dict = {}
    desc_dict = {}
    
    # rxnorm
    label_dict['rxnorm'] = {}
    desc_dict['rxnorm'] = {}
    conso_path = 'rxnorm/rrf/RXNCONSO.RRF'
    conso_cols = ['RXCUI','LAT','TS','LUI','STT','SUI','ISPREF','RXAUI','SAUI','SCUI','SDUI','SAB','TTY','CODE','STR','SRL','SUPPRESS','CVF']
    setting = {'RXCUI': str, 'LAT':str, 'SUPPRESS': str, 'STR':str, 'SAB':str}
    table = pd.read_csv(conso_path, names=conso_cols, usecols=setting.keys(), sep='|',
            dtype=setting, index_col=False)
    
    table = table.loc[(table['LAT'] == 'ENG')]
    table = table.loc[(table['RXCUI'].isin(code_set))]
    table.drop_duplicates('RXCUI', keep='first', inplace=True)
    
    for line in table[['RXCUI', 'STR']].itertuples(False):
        label_dict['rxnorm'][line[0]] = line[1]
        
    table = pd.read_csv('rxnorm\label.csv', dtype=str, index_col=False)
    for line in table.itertuples(False):
        label_dict['rxnorm'][line[0]] = line[1]
    
    # icd9 icd10 
    label_dict['icd9'] = {}
    desc_dict['icd9'] = {}
    label_dict['icd10'] = {}
    desc_dict['icd10'] = {}
    cols = ['icd_code','icd_version','long_title']
    table = pd.read_csv('mimic/hosp/d_icd_diagnoses.csv', dtype='str', index_col=False)
    table = table.loc[table['icd_code'].isin(code_set)]
    
    t1 = table.loc[table['icd_version'] == '10']
    for line in t1[['icd_code', 'long_title']].itertuples(False):
        label_dict['icd10'][line[0]] = line[1]
        
    t2 = table.loc[table['icd_version'] == '9']
    for line in t2[['icd_code', 'long_title']].itertuples(False):
        label_dict['icd9'][line[0]] = line[1]
    
    # phecode
    label_dict['phecode'] = {}
    desc_dict['phecode'] = {}
    table = pd.read_csv('icd2phecode/Phecode_map_v1_2_icd10cm_beta.csv',
                           dtype='str', index_col=False, encoding='unicode_escape')
    for line in table[['phecode', 'phecode_str']].itertuples(False):
        label_dict['phecode'][line[0]] = line[1]

    table = pd.read_csv('icd2phecode/phecode_icd9_rolled.csv',
                          index_col=False, dtype='str')
    for line in table[['PheCode', 'Phenotype']].itertuples(False):
        label_dict['phecode'][line[0]] = line[1]
    
    # drg
    label_dict['drg'] = {}
    desc_dict['drg'] = {}
    drg_path = 'data/drg_definition.txt'
    drg_cols = ['DRG', 'MDC', 'MS', 'Description']
    table = pd.read_csv(drg_path, skiprows=10,
            dtype='str', index_col=False, sep='\t')
    
    # table = table.loc[(table['DRG'].isin(code_set))]
    for line in table.itertuples(False):
        label_dict['drg'][line[0][0:3]] = line[0][11:]
    
    # ccs
    label_dict['ccs'] = {}
    desc_dict['ccs'] = {}
    path = 'ccs/CCS_services_procedures_v2021-1.csv'
    cols = ['Code Range','CCS','CCS Label']
    setting = {'CCS':'str', 'CCS Label':'str'}
    table = pd.read_csv(path, usecols=setting.keys(), skiprows=1,
                dtype=setting, index_col=False)
    
    table = table.loc[(table['CCS'].isin(code_set))]
    for line in table[['CCS', 'CCS Label']].itertuples(False):
        label_dict['ccs'][line[0]] = line[1]
    
    use = ["'CCS CATEGORY'", "'CCS CATEGORY DESCRIPTION'"]
    table = pd.read_csv('icd10pcs2ccs/ccs_pr_icd10pcs_2020_1.csv', usecols=use,
                           dtype='str', encoding='utf8', index_col=False)
    table.rename({use[0]: 'ccs', use[1]: 'label'}, axis=1, inplace=True)
    table.loc[:, 'ccs'] = table.loc[:, 'ccs'].apply(lambda x:x.replace('\'', '').strip())
    table.loc[:, 'label'] = table.loc[:, 'label'].apply(lambda x:x.replace('\'', '').strip())

    for line in table[['ccs', 'label']].itertuples(False):
        if line[0] not in label_dict['ccs']:
            label_dict['ccs'][line[0]] = line[1]
    
    # mimic
    label_dict['mimic'] = {}
    desc_dict['mimic'] = {}
    path = 'mimic/hosp/d_labitems.csv'
    cols = ['itemid','label','fluid','category','loinc_code']
    setting = {'itemid':str,'label':str,'fluid':str,'category':str}
    table = pd.read_csv(path, usecols=setting.keys(), 
                dtype=setting, index_col=False)
    
    table = table.loc[(table['itemid'].isin(code_set))]
    for line in table.itertuples(False):
        label_dict['mimic'][line[0]] = line[1]
        desc_dict['mimic'][line[0]] = line[2] + ' / ' + line[3]
    
    path = 'mimic/icu/d_items.csv'
    cols = ['itemid','label','abbreviation','linksto','category','unitname','param_type',
            'lownormalvalue','highnormalvalue']
    setting = {'itemid':str,'label':str,'category':str}
    table = pd.read_csv(path, usecols=setting.keys(), 
                dtype=setting, index_col=False)
    
    table = table.loc[(table['itemid'].isin(code_set))]
    for line in table.itertuples(False):
        label_dict['mimic'][line[0]] = line[1]
        desc_dict['mimic'][line[0]] = line[2]
    
    label_dict['transfer'] = {}
    desc_dict['transfer'] = {}
    return label_dict, desc_dict


def _add_icu_info(patients: pd.DataFrame):
    '''
    Add a column "los" to patients.csv, 
    which indicates the total length of stay in ICU (unit: days)
    
    Parameters:
    ----
        patients:
            the table patients_dict
            
    Returns:
    ----
        the table patients.csv with "los" column
    '''
    
    print('================================')
    print('Add ICU stay info to patients.csv')
    icu_stay = pd.read_csv(MIMIC_DIR + 'icu/icustays.csv', dtype={'subject_id':'str'}, index_col=False)
    
    print('icustays.csv shape', icu_stay.shape)
    
    # print((icu_stay['los'] <= 0).value_counts())
    
    icu_stay = icu_stay.loc[:,['subject_id','los']].groupby('subject_id').sum()
    print('Number of patients once in ICU:', icu_stay.shape)
    
    patients = patients.join(icu_stay, 'subject_id', how='left')
    
    print('Number of patients once in ICU (final):', patients[~patients['los'].isna()].shape[0])
    print('================================')
    
    return patients


def add_dict_category(input_dict_path, output_dict_path):
    '''
    Add the category column for the dictionary. 
    This function needs extra data files to run, so you may not be able to run it on your machine.
    The dictionary with category column is provided directly.
    
    Parameters:
    ----
        input_dict_path:
            filepath of original dictionary
        output_dict_path:
            filepath of updated dictionary
            
    Returns:
    ----
        None
    '''
    
    print("Adding categories the dictionary...")
    
    # load the original dictionary
    dictionary = pd.read_csv(input_dict_path, dtype='str', index_col=False)
    
    # get categories
    code_set = set(dictionary['code'])
    category_dict = _get_category_dict(code_set)
    
    # add categories
    category_col = []
    for code, code_type in dictionary[['code','code_type']].itertuples(False):
        if code_type in category_dict and code in category_dict[code_type]:
            category_col.append(category_dict[code_type][code])
        else:
            category_col.append('')
    
    dictionary['category'] = category_col
    
    # output dictionary
    dictionary.to_csv(output_dict_path, index=False)
    

def _get_category_dict(code_set:set):
    category_dict = {
        'mimic': {},
        'rxnorm': {},
        'phecode': {}
    }
    
    # add loinc for lab
    path = 'mimic/hosp/d_labitems.csv'
    cols = ['itemid','label','fluid','category','loinc_code']
    setting = {'itemid':str,'loinc_code':str}
    table = pd.read_csv(path, usecols=setting.keys(), 
                dtype='str', index_col=False)
    
    table = table.loc[(table['itemid'].isin(code_set))]
    for code, loinc in table.itertuples(False):
        if not pd.isna(loinc):
            category_dict['mimic'][code] = 'LOINC:' + loinc
    
    # add ingredient for RxNorm
    ingredient_set = _ingredient_level()
    for ing in ingredient_set:
        category_dict['rxnorm'][ing] = 'ingredient-level'
    
    # add Phenotype for PheCodes
    table = pd.read_csv('icd2phecode/phecode_definitions1.2.csv',
                           dtype='str', index_col=False, encoding='unicode_escape')
    
    for code, cat in table[['phecode', 'category']].itertuples(False):
        if not pd.isna(cat):
            category_dict['phecode'][code] = 'category:' + cat
    
    return category_dict


def _ingredient_level():
    table = pd.read_csv('rxnorm/ingredient.csv', index_col=False)
    
    print('total ingredient table size:', table.shape[0])
    
    table = set(table['ingredient'].dropna())
    
    print('final ingredient size:', len(table))
    
    return table


def main(args):
    generate_patient_dict(RESULT_ROOT_DIR + 'tuples.csv', RESULT_ROOT_DIR + 'patients_dict.csv')
    
    revise_code_dict(IDX_DIR + 'code_dict.csv', RESULT_ROOT_DIR + 'tuples.csv', 
                     RESULT_ROOT_DIR + 'code_dict.csv', add_label=args.add_label)
    
    if args.add_category:
        add_dict_category(RESULT_ROOT_DIR + 'code_dict.csv', RESULT_ROOT_DIR + 'code_dict_cat.csv')
    

if __name__=='__main__':
    main()
