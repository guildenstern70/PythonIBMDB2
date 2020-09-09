#
# DB2 Check
# A Python program to connect with IBM DB2 on Cloud
#

import ibm_db
import ibm_db_dbi
import yaml


def read_db_params():
    with open(r'parameters.yaml') as file:
        fruits_list = yaml.load(file, Loader=yaml.FullLoader)
    return fruits_list


def db2_connect(database, host, port, username, password):
    database = "DATABASE=%s;" % database
    host = "HOSTNAME=%s;" % host
    port = "PORT=%s;" % port
    username = "UID=%s;" % username
    password = "PWD=%s;" % password
    conn_string = database + host + port + 'PROTOCOL=TCPIP;' + username + password
    print(conn_string)
    connection = ibm_db.connect(conn_string, '', '')
    return ibm_db_dbi.Connection(connection)


def select(sql, conn):
    cursor = conn.cursor()
    cursor.execute(sql)
    for r in cursor.fetchall():
        print(r)


if __name__ == '__main__':
    print('IBM DB2 Connection Test....')
    print()
    params = read_db_params()
    _connection = db2_connect(params['database'], params['host'],
                              params['port'], params['user'], params['pwd'])
    _sql = """
            SELECT *
            FROM FSMTST.ALARM
            FETCH FIRST 20 ROWS ONLY;
    """
    select(_sql, _connection)
