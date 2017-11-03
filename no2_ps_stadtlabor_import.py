import csv
import logging
import datetime
import argparse
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
import time
import sys
import configparser
import os
from time import strftime

config = configparser.ConfigParser()
configf = 'no2_ps_stadtlabor_import.cfg'

# config.read('no2_ps_stadtlabor_import.cfg')

config.read(configf)

logfilename = config.get('SETTINGS', 'logfilename')
loglevel = config.get('SETTINGS', 'loglevel')


# logging Information
log = os.getenv('HOME') + '/' + 'logs' + '/' + logfilename + strftime("_%d_%m_%Y.log")
logging.basicConfig(filename=log,filemode='w',level=logging.DEBUG)
logger = logging.getLogger(__name__)


# retrieve import file name

# --importfname=no2db_slb_t2.csv

parser = argparse.ArgumentParser(description='get import filename')
parser.add_argument('--importfname', type=str, help='csv filename for import', required=True)

args = parser.parse_args()
fname = args.importfname

if fname:
    pass
else:
    errtxt = "No file defined for import"
    logger.error(errtxt)


# open import file for reading otherwise write error and exit

try:
    o = open(fname)
except  Exception :
    errtxt = "could not open file {0} ".format(fname)
    logger.error(errtxt,exc_info=True)
    sys.exit(1)

# database access paramaters

flavour = 'mysql'
username = 'no2db_slb'
password = 'Z1aRoUmsYOwDfwwB011S'
host = 'sherlock'
database = 'no2db_slb'

# DB connection and  table mapping


from sqlalchemy import create_engine

try:
    engine = create_engine("{0}://{1}:{2}@{3}/{4}".format(flavour, username, password, host, database),echo=False)
    # engine.execute(sql)
except  :
    errtxt = "failed to access DB incorrect connection paramaters  "
    logger.error(errtxt,exc_info=True)
    sys.exit(1)



 # for testing set to True to output sql

# map database table to object definition

metadata = MetaData(engine)

psdb_measurement = Table('psdb_measurement', metadata, autoload=True)


class Measurement(object):
    def __init__(self, station_id, date, value, comment, created_at, updated_at):
        self.station_id = station_id
        self.date = date
        self.value = value
        self.comment = comment
        self.created_at = created_at
        self.updated_at = updated_at


mapper(Measurement, psdb_measurement)

# Sessionmaker: Setup  object for creating sessions.

Session = sessionmaker(bind=engine, autoflush=True, autocommit=False,
                       expire_on_commit=True)

'''
Dictionary of Station ID's keyed via Spreadsheet column numbers.

'''
Stationskeyed = {}

'''
List of Station id's used to test for any duplicates whilst traversing the coulmn headings for stations
'''

stations = []
'''
dictionary of data extracted from spreadsheet
contains the complete list of values
i.e for each year, month , station id has the Value and Comment
'''
measurementdata = {}

yearmnth = []  # Year Month contactinated used to capture Duplicated

yr = ''  # Year var to keep track whilst traversing the row
mt = ''  # Month Var to keep track whilst traversing the row
yrmt = ''
counter = 1  # counter for dictionary keys


def numchk(chkstr):  # Check Spreadsheet values for numeric
    try:
        float(chkstr)
        return True
    except:
        return False


# read each row in csv file , validate data and create dictionary of measurment data

for rownum, line in enumerate(csv.reader(open(fname).readlines()[1:]), start=1):
    lastval = ''

    for col, columnvalue in enumerate(line):

        # 1st data row ensure no duplicate stations and create a Dictionary of Station ID's keyed via Spreadsheet column numbers
        if rownum == 1:
            if col > 1:
                if col % 2 == 0:
                    if columnvalue in stations:
                        errtxt = "Column Value Error duplicate Station Id found {0} ".format(columnvalue)
                        logger.error(errtxt)
                else:
                    Stationskeyed[col] = columnvalue
                    stations.append(columnvalue)

                    # Check Year month Columns are present , are numeric and not duplicated

        if rownum > 1:
            # year column

            if col == 0:
                if numchk(columnvalue):
                    yr = columnvalue
                else:
                    errtxt = "Row {1} Value Error , Incorrect Year  : {0} ".format(columnvalue, rownum + 1)
                    logger.error(errtxt)
                    sys.exit(1)
        # month column , ensure data is numeric

            if col == 1:
                mt = columnvalue

                if numchk(columnvalue):
                    if int(columnvalue) <= 12 and int(columnvalue) >= 1 :
                        yrmt = yr + mt.zfill(2)
                    else:
                        errtxt = "Row {1} Value Error , Incorrect Month : {0} ".format(columnvalue, rownum + 1)
                        logger.error(errtxt)
                        sys.exit(1)
                else:
                    errtxt = "Row {1} Value Error , Incorrect Month : {0} ".format(columnvalue, rownum + 1)
                    logger.error(errtxt)
                    sys.exit(1)


                if yrmt in yearmnth:
                    errtxt = "Row {1} duplicate Year Month combination found for {0} ".format((yr + mt.zfill(2)),
                                                                                          rownum + 1)
                    logger.error(errtxt)
                    sys.exit(1)
                else:
                    yearmnth.append(yrmt)  # add to list of Year/Month Combinations

                # Read each columnn of data and build a dictionary of values for each station ID Year Month Value and Comment

        # remaining columns


            if col > 1:

                '''check each column whilst
                traversing row determine if the column is 'Amount' or
                'Comment'Stationskeyed list contains columns for amount data only '''

                stn = Stationskeyed.get(col, None)

            # column with measuring value

                if stn is None:

                    '''found station column which has measuring value so we should check value is
                    numeric and save station id for writing to temp dictionary'''


                    if columnvalue:

                        if numchk(columnvalue):
                        # buffer the value (will be combined with its comment in the else clause)
                            lastval = columnvalue
                        else:
                            errtxt = "Value not Numeric Row: {4} Column: {5} YR: {0} Month {1} Station Id {2}  Column Value {3}".format(
                                yr, mt, stn, columnvalue, rownum, col + 1)
                            logger.error(errtxt)
                            sys.exit(1)
                else:
                # create dictionary of all data requiring update in DB
                    measurementdata[counter] = {'yrmonth': yrmt, 'station': stn, 'value': lastval, 'comment': columnvalue}
                    counter += 1
                    lastval = ''
                    stationid = ''

tstart =   time.time() ### time as at now

sess = Session()  # create a new an individual session object bound to engine (DB)

#  read dictionary contectents amd Delete all matching rows

deleted = 0

for a, b in measurementdata.items():

    # first step in DB remove all matching itmes , read dictionary , query db

    station = measurementdata[a]['station']
    measuredate = datetime.datetime.strptime(measurementdata[a]['yrmonth'] + '01', '%Y%m%d').date()
    value = measurementdata[a]['value']
    comment = measurementdata[a]['comment']

    try:
        query = sess.query(Measurement.id, Measurement.station_id, Measurement.value
                           , Measurement.date).filter_by(date=measuredate,
                                                         station_id=station).one()

        # data for stations that were removed from csv remain  in DB,(tend - tstart)
        delete = sess.query(Measurement.id, Measurement.station_id, Measurement.value
                            , Measurement.date).filter_by(date=measuredate,
                                                          station_id=station).delete()
        deleted += 1

    except NoResultFound:
        pass  # expected for new monthly row .. so pass
    except MultipleResultsFound:
        pass  ## really ???? is it possible for duplicates .. not sure
    except  Exception  as e:
        logger.error(errtxt, str(e))

# next step insert back into DB new values

added = 0

for a, b in measurementdata.items():
    # Second step in DB Insert dictionary data into table

    # engine.echo = True
    station = measurementdata[a]['station']
    measuredate = datetime.datetime.strptime(measurementdata[a]['yrmonth'] + '01', '%Y%m%d').date()
    value = measurementdata[a]['value']
    comment = measurementdata[a]['comment']

    newrow = Measurement(station, measuredate, value, comment,

                         created_at=datetime.date.today(), updated_at=datetime.date.today())

    try:
        sess.add(newrow)
        added += 1
    except  Exception  as e:
        logger.error(errtxt, str(e))


# complete transaction via commit and close the session
sess.commit()

logging.info('Import File Name :' + fname + ' Date: ' + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")))


logging.info(str(added) + ' rows added,'   +  str(deleted) + ' rows deleted, ' +\
              'Time elapsed= ' + format((time.time() - tstart),'.2f') + ' seconds.')


sess.close()

exit(0)