import psycopg2
import os
import csv

# # решение по разбиению скрипта миграции данных было принято? чтобы иметь возможность запустить миграцию не входя в виртуальную среду transfer_db_venv
# полученные в Bash-часте csv-файлы переносим в БД Postgre, создавая или дополняя ранее созданные одноименные таблицы
def transfer_to_IUM(database, user, password, host, port, table_name, file_name):
    conn_params = {
        "host": host,
        "port": port,
        "dbname": database,
        "user": user,
        "password": password
    }
    flag = True
    try:
        conn = psycopg2.connect(**conn_params)
        curs = conn.cursor()
    except Exception as e:
        flag = False
        print(f"Connection to IUM failed: {e}")

    if flag:
        with open(f"{file_name}.csv", mode='r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=';')
            header_csv = next(reader)
            header = []
            for colum in header_csv:
                new_item = f"{colum} TEXT"
                header.append(new_item)
            header_string = ', '.join(header)
            try:
                table_create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({header_string});"
                curs.execute(table_create_query)
                conn.commit()
            except Exception as e:
                flag = False
                print(f"Create table failed: {e}")
            if flag:
                for row in reader:
                    try:
                        rep_st = "%s,"*len(header_csv)
                        rep_st = rep_st[:-1]
                        insert_query = f"INSERT INTO {table_name} VALUES ({rep_st})"
                        curs.execute(insert_query,row)
                        conn.commit()
                    except Exception as e:
                        print(f"Insert data failed: {e}")
    if conn:
        file.close()
        curs.close()
        conn.close()
    return True

# main
# variables for connect to IUM:
Database = '****'
User = '****'
Password = '****'
Host = '****'
Port = '5432'

Data_View = os.getenv('View')
Data_Table = os.getenv('Table')

File_Name = f"{Data_View}_{Data_Table}"
Table_Name = f"{Data_View}__{Data_Table}"

transfer_to_IUM(Database, User, Password, Host, Port, Table_Name, File_Name)
# удаление csv-файла после копирования из него данных в Postgre
os.remove(f"{File_Name}.csv")