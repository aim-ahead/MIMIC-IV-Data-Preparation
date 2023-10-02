# MIMIC-IV-Data-Preparation
Tutorial on processing raw EHR data with MIMIC-IV v1.0 dataset

## Overview

This tutorial provides a brief overview of EHR data and some of the data processing steps involved in getting the data research ready. Its recommended that you read through the items in order as listed below. 

**NOTE: We use [MIMIC-IV v1.0](https://physionet.org/content/mimiciv/1.0/) dataset for this tutorial. Users would need to be credentialed to access this data. More information can be found at the bottom of this [page](https://physionet.org/content/mimiciv/1.0/)**

<br/>

## Contents

#### 1. Intro_to_EHR_and_MIMIC_data.pdf
* A gentle introduction to EHR and MIMIC data


#### 2. MIMIC-IV_Data_Prep_V1.0_Documentation.pdf
* MIMIC-IV v1.0 Data summary
* Data preparation code documentation


#### 3. MIMIC-IV_Data_Preperation_V1.0
* Data preparation Python scripts and dependent roll up mapping files
* Includes Roll-up Guidance.ipynb notebook


#### 4. Cleaned_MIMIC-IV_data
* The final cleaned output files would be generated once the scripts are executed as described in Data preparation code documentation 


<br/>

## Quick Start
We strongly recommend going through the documentation before trying the pipeline below.

* Clone this repository
* Register and download MIMIC-IV v1.0 from [here](https://physionet.org/content/mimiciv/1.0/)
* On terminal interface, run the following to download the entire MIMIC-IV v1.0
> wget -r -N -c -np --user insert-physionet-username-here --ask-password https://physionet.org/files/mimiciv/1.0/
* Set the path for input data (MIMIC data), dependency and roll up files and output dir under \MIMIC-IV_Data_Preparation_V1.0\code\settings.py
* Run \MIMIC-IV_Data_Preperation_V1.0\code\clean_mimic.py


<br/>
