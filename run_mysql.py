import mysql.connector
from mysql.connector import Error
import os

# configure following values
database_prefix = "testemail"
database_host = "localhost"
database_username = "root"
database_password = "root"
parent_dir = "/home/buddhi/Documents/product_migration/packs/4.1.0/rc3/CP/Axonect-Monetiser-2.0.0-RC3/dbscripts"


# configuration end

def create_database(database_array):
    print("Start Creating databases")

    try:
        connection = mysql.connector.connect(
            host=database_host,
            user=database_username,
            password=database_password
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            print("Connected MySQL server info ", db_info)
            cursor = connection.cursor()

            for db_name in database_array:
                cursor.execute("CREATE DATABASE " + db_name)
                cursor.execute("USE " + db_name)

                if db_name == (database_prefix + "_activitidb") or db_name == (database_prefix + "_ratedb"):
                    print("skipping table creation of " + db_name)
                else:
                    sql_commands = read_sql_file(source_db_script(db_name))

                    for sql_command in sql_commands:
                        print('SQL command ===  ' + db_name + sql_command)
                        if not (sql_command and sql_command.strip()):
                            print("empty SQL command")
                        else:
                            cursor.execute(sql_command)

    except Error as error:
        print("Error while executing create statement ", error)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySql Connection is closed")


def read_sql_file(file_path):
    fd = open(file_path, 'r')
    sql_file = fd.read()
    fd.close()

    return sql_file.split(';')


def create_db_name(prefix, db_name_tuple):
    print("Start Creating databases names")
    db_name_array = []
    print("Appending prefix as" + prefix)

    for name in db_name_tuple:
        db_name_array.append(prefix + name)

    return db_name_array


def source_db_script(db_name):
    print("Start Creating databases source location " + db_name)
    path_list = {'apimgtdb': 'apimgt/mysql.sql',
                 'shareddb': 'mysql.sql',
                 'userstoredb': 'mysql.sql',
                 'ratedb': 'dep-hub/mysql/rate_db.sql',
                 'apistatsdb': 'dep-hub/mysql/stats_db.sql',
                 'depdb': 'dep-hub/mysql/dep_db.sql',}

    if not db_name.__contains__('activitidb'):
        db_name_suffix = path_list.get(db_name.split('_')[1])
        return os.path.join(parent_dir, db_name_suffix)


def main():
    db_name_tuple = ('_apimgtdb', '_shareddb', '_userstoredb', '_ratedb', '_apistatsdb', '_depdb', '_activitidb')
    db_names = create_db_name(database_prefix, db_name_tuple)
    create_database(db_names)
    print("database creation process completed !!!")


if __name__ == "__main__":
    main()
