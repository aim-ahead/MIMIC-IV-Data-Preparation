import sys
import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import json
import rolluptool
from settings import RESULT_ROOT_DIR, TUPLE_DIR, IDX_DIR, MIMIC_DIR, UOM_SRC


V_FREQ = 'value_frequency'
FREQ = 'total_frequency'

idx_cols = ['code','code_type',V_FREQ,FREQ,'source_table','unit_of_measurement','with_value']


def _output_dict(table:pd.DataFrame, tablename:str):
    '''
    Parameters:
    ----
        table:
            The dictionary to output
        tablename:
            tablename of the input/output file
            
    Returns:
    ----
        No return
    '''
    
    table = table.groupby(['code', 'code_type']).count()
    print('unknown freq', int(table.loc['<unk>', FREQ]) if '<unk>' in table.index else 0)
    if '<unk>' in table.index:
        table.drop(['<unk>'], inplace=True)
        
    table = table.loc[(table[FREQ] >= 1000)]
    
    table.sort_values(FREQ, inplace=True)
    table[V_FREQ] = 0
    table['source_table'] = tablename
    table['unit_of_measurement'] = ''
    table['with_value'] = 0
    table.to_csv(IDX_DIR + tablename + '_dict.dict', columns=idx_cols[2:], index_label=['code', 'code_type'])

    table.reset_index(inplace=True)
    all_type = table['code_type'].unique()
    for ctype in all_type:
        temp_table = table.loc[table['code_type'] == ctype]
        print('type', 'code_num', 'final', 'mean', 'median','max', 'min', sep='\t')
        print(ctype, temp_table.shape[0], temp_table[FREQ].sum(), int(temp_table[FREQ].mean()), 
            temp_table[FREQ].median(), temp_table[FREQ].max(), temp_table[FREQ].min(), sep='\t')
        print('')


def _procedures_icd_dict(tablename):
    '''
    Generate dictionary for procedures_icd.csv
    Called by function generate_ccs_dict()
    '''
    
    print('\ngenerating dict of', tablename)
    
    icd10pcs2css = rolluptool.get_icd10pcs2css()
    icd9cm2ccs = rolluptool.get_icd9cm2ccs()
    
    path = MIMIC_DIR + 'hosp/' + tablename + '.csv'
    cols = ['subject_id', 'hadm_id', 'seq_num', 'chartdate', 'icd_code', 'icd_version']
    setting = {'icd_code': str, 'icd_version':int}
    table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    table.rename({'icd_code':'code', 'icd_version':'code_type'}, axis=1, inplace=True)
    print('number of code before rolling up:', 
          table[['code', 'code_type']].drop_duplicates(keep='first', inplace=False).shape[0])
    
    table.loc[:, 'code'] = table.loc[:, ['code', 'code_type']].apply(
        lambda x:icd9cm2ccs.get(x[0], '<unk>') if x[1] == 9 else icd10pcs2css.get(x[0], '<unk>'), axis=1)
    
    table.loc[:, 'total_frequency'] = 1
    table.loc[:, 'code_type'] = 'ccs'
    
    return table


def _hcpcsevents_dict(tablename):
    '''
    Generate dictionary for hcpcsevents.csv
    Called by function generate_ccs_dict()
    '''
    
    print('\ngenerating dict of', tablename)
    
    diction = rolluptool.get_cpt2ccs()
    
    path = MIMIC_DIR + 'hosp/' + tablename + '.csv'
    cols = ['subject_id', 'hadm_id', 'chartdate', 'hcpcs_cd', 'seq_num', 'short_description']
    setting = {'hcpcs_cd': str}
    table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    table.rename({'hcpcs_cd':'code'}, axis=1, inplace=True)
    print('number of code before rolling up:', 
          table['code'].drop_duplicates(keep='first', inplace=False).shape[0])

    table.loc[:, 'code'] = table.loc[:, 'code'].apply(
        lambda x:diction.get(x, '<unk>'))
    table.loc[:, 'total_frequency'] = 1
    table.loc[:, 'code_type'] = 'ccs'
    
    return table


def generate_ccs_dict(tablename):
    '''
    Generate dictionary for hcpcsevents and procedures_icd (CCS Codes)
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
    t1 = _hcpcsevents_dict('hcpcsevents')
    t2 = _procedures_icd_dict('procedures_icd')
    
    t1 = t1.append(t2)
    _output_dict(t1, tablename)


def generate_drgcodes_dict(tablename):
    '''
    Generate dictionary for drgcodes.csv (DRG Codes)
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating dict of', tablename)
    
    path = MIMIC_DIR + 'hosp/' + tablename + '.csv'
    cols = ['subject_id', 'hadm_id', 'drg_type', 'drg_code', 'description', 'drg_severity', 'drg_mortality']
    setting = {'drg_code': 'str'}
    table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    table.rename({'drg_code':'code'}, axis=1, inplace=True)
    print('number of code before rolling up:', 
          table['code'].drop_duplicates(keep='first', inplace=False).shape[0])
    
    table.loc[:, 'total_frequency'] = 1
    table.loc[:, 'code_type'] = 'drg'
    
    _output_dict(table, tablename)
    
    
def generate_diagnoses_icd_dict(tablename):
    '''
    Generate a dictionary for diagnoses_icd.csv (PheCode).
    roll up: ICD -> PheCode
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating dict of', tablename)
    
    icd102phe = rolluptool.get_icd102phe()
    icd92phe = rolluptool.get_icd92phe()
    
    path = MIMIC_DIR + 'hosp/' + tablename + '.csv'
    cols = ['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'icd_version']
    setting = {'icd_code': str, 'icd_version':str}
    table = pd.read_csv(path, usecols=setting.keys(), dtype=setting, index_col=False)
    table.rename({'icd_code':'code', 'icd_version':'code_type'}, axis=1, inplace=True)
    print('number of code before rolling up:', 
          table[['code', 'code_type']].drop_duplicates(keep='first', inplace=False).shape[0])
    
    # roll up icd9 and icd10 respectively
    table9 = table.loc[table['code_type'] == '9'].copy()
    table10 = table.loc[table['code_type'] == '10'].copy()
    
    table9.loc[~table9['code'].isin(icd92phe), 'code_type'] = 'icd9'
    table9.loc[table9['code'].isin(icd92phe), 'code_type'] = 'phecode'
    table10.loc[~table10['code'].isin(icd102phe), 'code_type'] =  'icd10'
    table10.loc[table10['code'].isin(icd102phe), 'code_type'] = 'phecode'
    
    table9.loc[:, 'code'] = table9.loc[:, 'code'].apply(
        lambda x:(icd92phe[x] if x in icd92phe else x))
    table10.loc[:, 'code'] = table10.loc[:, 'code'].apply(
        lambda x:(icd102phe[x] if x in icd102phe else x))

    # merge the roll up result
    table = table9.append(table10)
    table.loc[:, 'total_frequency'] = 1
    
    _output_dict(table, tablename)


def generate_prescriptions_dict(tablename):
    '''
    Generate a dictionary for Rxnorm (prescriptions.csv).
    roll up: NDC -> Rxnorm
    
    Parameters:
    ----
        tablename:
            tablename of the file
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating dict of', tablename)
    
    ndc2rxnorm = rolluptool.get_ndc2rxnorm()
    
    path = MIMIC_DIR + 'hosp/prescriptions.csv'
    cols = ['subject_id', 'hadm_id', 'pharmacy_id', 'starttime', 'stoptime', 'drug_type', 'drug', 'gsn', 'ndc', 'prod_strength', 'form_rx', 'dose_val_rx', 'dose_unit_rx', 'form_val_disp', 'form_unit_disp', 'doses_per_24_hrs', 'route']
    
    setting = {'ndc':str}
    table = pd.read_csv(path, usecols=setting.keys(),
            dtype=setting, index_col=False)
    table.rename({'ndc':'code'}, axis=1, inplace=True)
    
    condition = (~table['code'].isna()) & (table['code'] != '0') & \
        (table['code'].str.len() == 11) & (table['code'].isin(ndc2rxnorm))

    table = table[condition]
    print('freq before rolling up:', table.shape)

    print('number of code before rolling up:', 
          table['code'].drop_duplicates(keep='first', inplace=False).shape[0])
    
    table.loc[:, 'code'] = table.loc[:, 'code'].apply(
        lambda x:ndc2rxnorm.get(x, '<unk>')) 
    
    table.loc[:, 'total_frequency'] = 1
    table.loc[:, 'code_type'] = 'rxnorm'
    _output_dict(table, tablename)


def generate_transfers_dict(tablename):
    '''
    Generate a dictionary for transfers.csv
    
    Parameters:
    ----
        tablename:
            Indicate the name of table
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating dict of', tablename)
    
    path = MIMIC_DIR + 'core/{}.csv'.format(tablename)
    
    setting = {'eventtype':str, 'hadm_id':str}
    table = pd.read_csv(path, usecols=setting.keys(),
            dtype=setting, index_col=False)
    table.rename({'eventtype':'code'}, axis=1, inplace=True)
    
    table.loc[:, 'total_frequency'] = 1
    table.loc[:, 'code_type'] = 'transfer'
    _output_dict(table, tablename)


def generate_no_value_dict(tablename):
    '''
    Generate a dictionary for procedureevents.csv and inputevents.csv respectively
    
    Parameters:
    ----
        tablename:
            Indicate the name of table (procedureevents/inputevents)
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating dict of', tablename)
    
    path = MIMIC_DIR + 'icu/{}.csv'.format(tablename)
    
    setting = {'itemid':str}
    table = pd.read_csv(path, usecols=setting.keys(),
            dtype=setting, index_col=False)
    table.rename({'itemid':'code'}, axis=1, inplace=True)
    
    table.loc[:, 'total_frequency'] = 1
    table.loc[:, 'code_type'] = 'mimic'
    _output_dict(table, tablename)


def generate_value_dict(tablename='outputevents', filedir='icu', value_col='valuenum'):
    '''
    Generate a dictionary for labevents, chartevents, and outputevents respectively
    
    Parameters:
    ----
        tablename:
            Indicate the name of table (labevents/chartevents/outputevents)
        filedir:
            Indicate the directory of table (hosp/icu)
        value_col:
            The column containing value of code (value/valuenum)
            
    Returns:
    ----
        No return
    '''
    
    print('\ngenerating dict of', tablename)
    
    # a dictionary to record frequency of all codes
    freq_record = {}
    
    # if a lab code always occurs with the same value, 
    # we regard it as a code without value
    value_record = {}
    
    # a dictionary to normalize units
    with open(UOM_SRC + '{}_uom_dict.json'.format(tablename), 'r', encoding='utf8') as f:
        uom_dict = json.load(f)
        uom_dict = {int(k):v for k,v in uom_dict.items()}
    
    # load the source table
    src_path = MIMIC_DIR + '{}/{}.csv'.format(filedir, tablename)
    setting = {'itemid':int, value_col:float, 'valueuom':str}
    with pd.read_csv(src_path, usecols=setting.keys(), index_col=False,
            chunksize=30000000) as reader:
        for i, chunk in enumerate(reader):
            for itemid, valuenum, valueuom in tqdm(chunk.itertuples(False), total=chunk.shape[0]):
                # 0:itemid, 1:valuenum, 2:valueuom
                if itemid not in uom_dict:
                    continue
                
                if itemid not in freq_record:
                    freq_record[itemid] = {}
                    freq_record[itemid]['value'] = 0
                    freq_record[itemid]['total'] = 0
                
                # count codes
                freq_record[itemid]['total'] += 1
                
                # normalize unit of measurement
                unit = _normalize_unit(valueuom)
                
                if not pd.isna(valuenum):
                    if '<main>' in uom_dict[itemid]:
                        final_value = None
                        if valuenum == 0:
                            freq_record[itemid]['value'] += 1
                            final_value = 0
            
                        elif uom_dict[itemid]['<main>'] != unit:
                            # code with value and appropriate unit of measurement
                            if unit in uom_dict[itemid] and uom_dict[itemid][unit] != 0:
                                freq_record[itemid]['value'] += 1
                                final_value = valuenum * uom_dict[itemid][unit]
                        else:
                            freq_record[itemid]['value'] += 1
                            final_value = valuenum
                    
                    # check whether a code always occurs with the same value
                    if final_value != None:
                        if itemid in value_record:
                            if value_record[itemid] != None and value_record[itemid] != final_value:
                                    value_record[itemid] = None
                        else:
                            value_record[itemid] = final_value

    table = []
    for k, v in freq_record.items():
        if v['total'] >= 1000:
            if v['value'] >= 1000 and value_record[k] == None:
                table.append([k, v['value'], v['total'], 1])
            else:
                table.append([k, 0, v['total'], 0])
            
    table = pd.DataFrame(table, 
            columns=['code', V_FREQ, FREQ, 'with_value']).sort_values('code')
    
    def change_uom(x):
        if '<main>' in uom_dict[x]:
            return uom_dict[x]['<main>']
        else:
            return 'none'
    
    # fill in other columns of the dictionary table
    table['unit_of_measurement'] = table['code'].apply(change_uom)
    table['unit_of_measurement'] = table['unit_of_measurement'].apply(_normalize_unit)
    table.loc[table['with_value'] == 0, 'unit_of_measurement'] = ''
    table['source_table'] = tablename
    table['code_type'] = 'mimic'

    table = table.loc[:, idx_cols]
    table.sort_values(['with_value', FREQ], inplace=True)
    table.to_csv(IDX_DIR + tablename + '_dict.dict', index=False)


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
        

def remove_duplicate_codes():
    '''
    Remove duplicate codes between chartevents and labevents
    '''
    
    print('Removing duplicate codes between chartevents and labevents...')
    
    dup = pd.read_csv(MIMIC_DIR + 'icu/d_items.csv',
        usecols=['itemid', 'linksto','category'], dtype=str, index_col=None)
    
    dup = dup[(dup['linksto'] == 'chartevents') & (dup['category'] == 'Labs')]
    print('Number of duplicate codes:', dup.shape[0])
    
    dictionary = pd.read_csv(IDX_DIR+'chartevents_dict.dict', dtype=str, index_col=False)
    
    dup = set(dup['itemid'])
    dictionary = dictionary[(dictionary['source_table'] != 'chartevents') | ~dictionary['code'].isin(dup)]
    
    dictionary.to_csv(IDX_DIR+'chartevents_dict.dict', index=False)


def merge_dict(out_path):
    '''
    Merge dictionaries of all tables together.
    
    Parameters:
    ----
        out_path: filepath to output the merged dictionary
            
    Returns:
    ----
        No return
    '''
    
    print('Merging all dictionaries together...')
    
    table = pd.DataFrame(columns=idx_cols)
    
    for tablename in os.listdir(IDX_DIR):
        if '.dict' in tablename:
            path = IDX_DIR + tablename
            temp = pd.read_csv(path, dtype={'code':'str'}, index_col=False)
            table = pd.concat((table, temp), ignore_index=True)
    
    # sort al entries
    # table.sort_values(['with_value', 'total_frequency'], inplace=True, ignore_index=True)
    table.sort_values(['code_type', 'with_value', 'total_frequency'], inplace=True, ignore_index=True)
    table.index += 1
    
    # statistics
    value_table = table.loc[table['with_value'] == 1]
    print('ratio:', value_table[V_FREQ].divide(value_table[FREQ]).mean())
    
    print('total value:', table[V_FREQ].sum(), 'total freq', table[FREQ].sum())
    # table.rename({V_FREQ:'value_frequency'}, inplace=True)
    
    print('all:')
    print('code_num', 'final', 'mean', 'median','max', 'min', sep='\t')
    print(table.shape[0], table[FREQ].sum(), int(table[FREQ].mean()), 
          table[FREQ].median(), table[FREQ].max(), table[FREQ].min(), sep='\t')
    print('-'*20)
    
    print('value:')
    print('code_num', 'final', 'mean', 'median','max', 'min', sep='\t')
    print(value_table.shape[0], value_table[V_FREQ].sum(), int(value_table[V_FREQ].mean()), 
          value_table[V_FREQ].median(), value_table[V_FREQ].max(), value_table[V_FREQ].min(), sep='\t')
    print('-'*20)
    
    # output dict
    table.to_csv(out_path, index_label='index')

def main():
    # generate a dictionary for each table
    generate_prescriptions_dict('prescriptions')
    generate_ccs_dict('ccs')
    generate_drgcodes_dict('drgcodes')
    generate_diagnoses_icd_dict('diagnoses_icd')
    generate_transfers_dict('transfers')

    generate_no_value_dict('procedureevents')
    generate_no_value_dict('inputevents')

    generate_value_dict('outputevents', 'icu', 'value')
    generate_value_dict('labevents', 'hosp', 'valuenum')
    generate_value_dict('chartevents', 'icu', 'valuenum')

    # remove duplicate codes between chartevents and labevents
    remove_duplicate_codes()

    # merge all the dictionaries together
    merge_dict(IDX_DIR + 'code_dict.csv')


if __name__=='__main__':
    main()
    

 