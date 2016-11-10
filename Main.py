import MySQLdb
import datetime
from time import time
import PriceListCleanUp as plc
import Creds as c


class MGRError(Exception):
    # Main exception class
    response = None

def connect_to_db():

    #using old method:
    # need to create a separate file called 'Creds' that contains passwords for DB
    mydb = MySQLdb.connect(host= c.host, user= c.user, passwd= c.passwd, db= c.db)

    return (mydb)

def closeCon(mydb):
    mydb.commit()
    mydb.close()

    print ("Done, closed connection to DB")

if __name__ == '__main__':

    print ('Running...')

    mydb = connect_to_db()

    entry_date = datetime.datetime.now()

    plist = plc.PriceListCleanUp(raw_file_name = '***enter file name***',
                                raw_data_folder= '***enter folder address***',
                                SupplierID= 5366260,
                                current_as_of_date= datetime.date(2016,11,9),
                                entry_date= entry_date)

    #available options:
    plist.cleanList(mydb)
    plist.to_db(mydb)
    plist.to_csv()

    start_time = time()

    closeCon(mydb)

    print (('Run time: {}').format((time() - start_time)))
