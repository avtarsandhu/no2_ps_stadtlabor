 SLB (Stadtlabor Bern) NO2 PSDB - Importskript
> Data Importer from Importer in Python3 from CSV to Database


Import script using  Python3 from supplied CSV to Database

Sample data (import_files directory):

no2db_slb_t1.csv   small dataset to test basic load
no2db_slb_t2.csv   small dataset includes erroneus data
no2db_slb_t3_large.csv large dataset


steps:

- load/parse CSV
- match stations in CSV with stations in DB (via ID row[?])
- Check Spreadsheet values for numeric , read  each row in csv file
  and validate data. Ensure no duplicate stations  , Check Year month Columns are present , are numeric and not duplicated.
- insert/update into DB
- check back DB status with parsed data. complete transaction via
  commit
- launch on file arrival (via FTP), probably with incron - TBC




## Installation

OS X & Linux:

```sh

```


## Usage example

configuration  file no2_ps_stadtlabor_import.cfg  contains name logfile name and Debug level
Import file name passed as argument --importfname
logfile stored in ('HOME') + '/' + 'logs' + '/' + logfilename + strftime("_%d_%m_%Y.log")


Database :


flavour = 'mysql'
username = 'no2db_slb'
host = 'sherlock'
database = 'no2db_slb'




## Development setup

Describe how to install all development dependencies and how to run an automated test-suite of some kind. Potentially do this for multiple platforms.

```sh
```
mysqlclient (for DB connection Check)
SQLAlchemy  (for DB data Load )



## Release History


* 0.1.0
    * first release



## Meta

Avtar Sandhu  – 2020sandhu@gmail.com


(https://gitlab.mt.local/ASA/SLB_NO2PSDB_Importskript)
