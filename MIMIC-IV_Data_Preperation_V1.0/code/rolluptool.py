import sys
import os
import numpy as np
import pandas as pd
from settings import ROLL_UP_SRC
from typing import Dict


def get_cpt2ccs() -> Dict[str, str]:
    d = _get_rollup(ROLL_UP_SRC + 'cpt2ccs_rollup.csv')
    return d


def get_ndc2rxnorm() -> Dict[str, str]:
    d = _get_rollup(ROLL_UP_SRC + 'ndc2rxnorm_rollup.csv')
    return d


def get_icd92phe() -> Dict[str, str]:
    d = _get_rollup(ROLL_UP_SRC + 'icd92phe_rollup.csv')
    return d

def get_icd102phe() -> Dict[str, str]:
    d = _get_rollup(ROLL_UP_SRC + 'icd102phe_rollup.csv')
    return d

def get_icd9cm2ccs() -> Dict[str, str]:
    d = _get_rollup(ROLL_UP_SRC + 'icd9cm2ccs_rollup.csv')
    return d

def get_icd10pcs2css() -> Dict[str, str]:
    d = _get_rollup(ROLL_UP_SRC + 'icd10pcs2ccs_rollup.csv')
    return d


def _get_rollup(file_path) -> Dict[str, str]:
    map_code = pd.read_csv(file_path, dtype='str', index_col=False)
    
    d = {}
    for line in map_code.itertuples(False):
        if line[0] in d:
            raise 'error'
        d[line[0]] = line[1]
        
    return d

    
    
    
    