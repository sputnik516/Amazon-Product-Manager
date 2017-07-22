from sqlalchemy import create_engine, exc
import PY.lib.creds as creds
from PY.lib.logging_to_sql import get_logger
from sqlalchemy.exc import ProgrammingError, OperationalError
import re
import time

# Set up logging
logger = get_logger('db')


def connect_to_db():
    try:
        mydb = 'mysql://' + creds.db_user + ':' + creds.db_passwd + '@' + creds.db_host + '/' + creds.db_name
        engine = create_engine(mydb)
        connection = engine.connect()

        return connection

    except OperationalError:
        time.sleep(5)
        connection = connect_to_db()
        return connection


def close_con(connection):

    connection.close()
    # print ("Done, closed connection to DB")


def df_to_sql_duplicate_update(df, db_name, db_table, con, chunk_size=500):
    """Pandas DataFrame to MySQL, on duplicate key update. Write to DB in chunks."""

    # Create connection unique to this query
    # con = connect_to_db()

    # Fill nan with NULL, for MySQL
    df.fillna('NULL', inplace=True)

    # Escape quotes
    df.replace({"'": "''"}, regex=True, inplace=True)

    cols = ','.join(list(df))
    num_cols = len(df.columns)
    if num_cols == 0:
        msg = 'No columns in DF'
        print(df)
        logger.info(msg)
        print(msg)

        return None

    # Reset index because it will conflicts with chunk size logic if the index is not: ix = 0 -->n, ix+1...
    df.reset_index(inplace=True, drop=True)

    x = 0
    while x <= len(df):
        end_ix = min(x + chunk_size, len(df)) - 1
        df_temp = df.ix[x:end_ix]

        x += chunk_size

        sql = """
        INSERT INTO {}.{}
            ({})
            VALUES
        """.format(db_name, db_table, cols)

        for index, row in df_temp.iterrows():
            temp_values = r'('
            for c in range(0, num_cols):

                # If Float, i.e. prices, no quotes around value
                if isinstance(row[c], float) or isinstance(row[c], bool) or row[c] == 'NULL':

                    temp_values += r"{},".format(str(row[c]))

                elif len(str(row[c])) == 0:

                    temp_values += r"NULL,"

                # If not Float, need quotes around value
                else:

                    temp_values += r"'{}',".format(str(row[c]))

            # Remove last comma
            temp_values = temp_values[:-1]
            temp_values += '),'

            # Append to SQL string
            sql += '\n' + temp_values

        # Remove last comma
        sql = sql[:-1]

        sql += '\n ' + 'ON DUPLICATE KEY UPDATE \n'

        for col in list(df):
            sql += '\n{} = VALUES({}),'.format(col, col)

        # Remove last comma
        sql = sql[:-1]

        # Remove non ASCII
        sql = re.sub(r'[^\x00-\x7F]', '', sql)
        # sql = sql.replace('\\', '')

        if len(df_temp) > 0:
            sql_to_db(sql=sql, con=con)
        else:
            msg ='Zero values to be written, not executing SQL'
            print(msg)

        # Close con
        # con.close()


def sql_to_db(sql, con):

    try:
        con.execute(sql)
        msg = 'SUCCESS writing to SQL'
        # logger.info(msg)

    except ProgrammingError as e:
        msg = 'ERROR writing to SQL: {}'.format(e)
        # logger.warning(msg)

    except exc.OperationalError as e:
        msg = "sqlalchemy.exc.OperationalError {}".format(e)
        # logger.warning(msg)

    except Exception as e:
        msg = 'Unexpected error in sql_to_db'
        print(msg)
        print(e)
        print(sql)
        # logger.warning(msg)

def make_sql(cols, vals, db_name, db_table):
    """
    :param cols: list of columns, ['col1', 'col2']...
    :param vals: list of lists, [['a', 'b', 'c', 44.32], ['d', 'e', 'f', 'NULL'], ['g', 'h', 'i', '234']]...
    :param db_name: str, 'test table'
    :param db_table: str, 'test db'
    :return: sql str
    """
    if not isinstance(vals, list):
        msg = 'vals must be a list, received {}'.format(type(vals))
        raise Exception(msg)
    if not isinstance(cols, list):
        msg = 'cols must be a list, received {}'.format(type(cols))
        raise Exception(msg)
    for row in vals:
        if not isinstance(row, list):
            msg = 'row in vals must be a list, received {} \nRow: {}'.format(type(row), row)
            raise Exception(msg)
    for v in vals:
        if len(v) != len(cols):
            msg = 'Number of columns needs to match number of values per row. \n' \
                 'Received {} values, expected {} \n Row: {}'.format(len(v), len(cols), v)
            raise Exception(msg)

    cols_str = ','.join(cols)
    sql = "INSERT INTO `{}`.`{}`({}) \nVALUES \n".format(db_name, db_table, cols_str)

    for row in vals:
        temp_sql = '\n('
        for col_num in range(0, len(cols)):
            if isinstance(row[col_num], float) or str(row[col_num]).upper() in ['NULL', 'FALSE', 'TRUE']:
                temp_sql += '{},'.format(row[col_num])
            else:
                temp_sql += "'{}',".format(row[col_num])

        temp_sql = temp_sql[:-1] + '),'
        sql += temp_sql

    # Remove last comma
    sql = sql[:-1]

    sql += '\n ' + 'ON DUPLICATE KEY UPDATE \n'

    for col in cols:
        sql += '\n`{}` = VALUES(`{}`),'.format(col, col)

    # Remove last comma
    sql = sql[:-1]

    # Remove non ASCII
    sql = re.sub(r'[^\x00-\x7F]', '', sql)

    return sql


def write_df_to_db_chunks(df, db_name, db_table, con, chunk_size=1000):

    x = 0
    while x <= len(df):
        end_ix = min(x + chunk_size, len(df))
        sql = df_to_sql_duplicate_update(df.ix[x:end_ix], db_name, db_table)
        x += chunk_size + 1

        sql_to_db(sql=sql, con=con)
