import pandas as pd
import datetime
import numpy as np

class PriceListCleanUp(object):

    #used to clean Price List files for import to the database

    def __init__(self, raw_file_name, raw_data_folder, SupplierID, current_as_of_date, entry_date):
        self.raw_file_name = raw_file_name
        self.raw_data_folder = raw_data_folder
        self.raw_data_path = (raw_data_folder + raw_file_name)
        self.SupplierID = SupplierID
        self.current_as_of_date = current_as_of_date
        self.entry_date = entry_date
        self.clean_data = pd.DataFrame() #empty DataFrame that will be replaced with processed data

    def cleanList(self, mydb):

        #opens raw file
        raw_file = pd.read_csv(self.raw_data_path, dtype={'UPC': str})

        # creates DataFrame 'clean_data"
        clean_data = pd.DataFrame(raw_file)

        # add additional info to dataframe:
        clean_data['SupplierID'] = self.SupplierID
        clean_data['Current_as_of_Date'] = self.current_as_of_date
        clean_data['Unique_Entry_Id'] = self.entry_date

        # check for consistency, length, remove non-digits:
        clean_data = self.remove_non_digits(clean_data)
        clean_data = self.upc_check(clean_data)
        clean_data = self.clean_SKU(clean_data)

        #overwrite the blank DataFrame with processed data:
        self.clean_data = clean_data

    def to_csv(self):

        # Output to csv file:
        self.clean_data.to_csv(('SupplierID-{}-{}.csv').format(self.SupplierID, self.entry_date.date()), index=None)

        print ('Added {} items from Price List to CSV file, Supplier ID: {}').format(len(self.clean_data.index),
                                                                               self.SupplierID)

    def to_db(self, mydb):

        # Output to database:
        self.clean_data.to_sql(con=mydb, name='items', if_exists='append', flavor='mysql', index=False, index_label='UPC')

        print ('Added {} items from Price List to DB, Supplier ID: {}').format(len(self.clean_data.index), self.SupplierID)

    def upc_check(self, clean_data):

        # removes rows with no UPC:
        clean_data = (clean_data[pd.notnull(clean_data['UPC'])])

        # check UPC code length. If 11 characters, adds '0' before.
        UPC_11_char = clean_data.UPC.astype(str).str.len() == 11
        clean_data.ix[UPC_11_char, 'UPC'] = '0' + clean_data[UPC_11_char]['UPC'].astype(str)

        # Remove non-digits from UPC's:
        clean_data['UPC'].replace(regex=True, inplace=True, to_replace=r'\D', value=r'')
        clean_data['Case_UPC'].replace(regex=True, inplace=True, to_replace=r'\D', value=r'')

        #remove rows with no UPCs:
        clean_data['UPC'].replace('', np.nan, inplace= True)
        clean_data.dropna(subset= ['UPC'], inplace= True)

        return (clean_data)

    def clean_SKU(self, clean_data):

        # fields to remove, characters that are not letters, numbers or (_), (-) characters
        rem_fields = (r'[^.0-9_a-zA-Z-_]')

        clean_data['Supplier_SKU'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')

        if ('Supplier_SKU2' in clean_data.columns):
            clean_data['Supplier_SKU2'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')

        return clean_data

    def remove_non_digits(self, clean_data):

        # fields to remove
        rem_fields = (r'[^.0-9]')

        # Remove non-numbers
        clean_data['Cost'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'0')
        clean_data['Cost2'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')
        clean_data['Cost3'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')
        clean_data['MSRP'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')
        clean_data['MAP'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')
        clean_data['Case_Pack'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')
        clean_data['Master_Case_Pack'].replace(regex=True, inplace=True, to_replace=rem_fields, value=r'')

        return clean_data






