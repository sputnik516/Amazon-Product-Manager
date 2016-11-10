# Amazon-Product-Manager
A tool to help high-volume Amazon retailers manage large numbers of items

'PriceListCleanUp.py' takes in a .csv file of different items, with column headings from 'Price list Data Base upload template.csv'.

Then, it cleans and formats the data into a format that can then be used to get listings and offers via the Amazon API. For example, it converts UPC codes to the proper format (10, 11, or 13 digits), removes unnecessary characters from SKU's, costs, etc.

This data can then be uploaded to a MySQL DB via 'to_db()' or saved as a .cvs file via 'to_csv()'
