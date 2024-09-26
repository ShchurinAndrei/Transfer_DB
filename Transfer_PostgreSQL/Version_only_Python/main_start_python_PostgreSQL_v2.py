import psycopg2
import os
import csv

# получение из базы токена для подключения к защищенной БД, название витрин и находящихся в них таблиц
def receiving_source_data(flag, database, user, password, host, port,  table_name):
    conn_params = {
        "host": host,
        "port": port,
        "dbname": database,
        "user": user,
        "password": password
    }
    authoriz_token = []
    data_view = []
    data_table = []
    try:
        conn = psycopg2.connect(**conn_params)
        curs = conn.cursor()
    except Exception as e:
        flag = False
        print(f"Connection to PostgreSQL failed: {e}")
    if flag:
        curs.execute(f"SELECT * FROM {table_name}")
        while True:
            row = curs.fetchone()
            if row is None:
                break
            authoriz_token.append(row[0])
            data_view.append(row[1])
            data_table.append(row[2])
    if conn:
        curs.close()
        conn.close()
    return flag, authoriz_token, data_view, data_table

# создание bash-скрипта (sh-файла) со строкой POST-запроса к защищенной БД
# (было применено такое решение из-за невыявленной ошибки при прямом обращение, к консоли линукс с текстом POST-запроса содержащим передаваемые переменные)
def create_bash_script(flag, authoriz_token, data_view, data_table, file_name):
    try:
        file = f'{file_name}.sh'
        with open(file, mode='w') as file:
            file.write("#!/bin/bash\n")
            file.write(f"curl --insecure -X POST 'https://****/API/TDS/v1/SQL_CSV/:FileName/:SaveOption?delimeter=;&charset=UTF-8_BOM' -H 'Content-Type: application/json' -H 'Authorization-Token: {authoriz_token}' -d 'SELECT * FROM \"{data_view}\".\"{data_table}\"' > {file_name}.csv")
            file.close()
        return flag
    except Exception as e:
        flag = False
        print(f"Create bash script failed: {e}")
        return flag

# запуск bash-скрипта с ожиданием его выполнения
def start_bash_script(flag, file_name):
    try:
        result = subprocess.run(['bash', f'{file_name}.sh'], check=True)
        return flag

    except Exception as e:
        flag = False
        print(f"Bash script crashed with an error: {e}")
        return flag
# в результате ожидаем получить csv-файл с названием: витрина_таблица

# полученные в предыдущем шаге csv-файлы переносим в БД Postgre, создавая или дополняя ранее созданные одноименные таблицы
def transfer_to_IUM(flag, database, user, password, host, port,  table_name, file_name):
    conn_params = {
        "host": host,
        "port": port,
        "dbname": database,
        "user": user,
        "password": password
    }
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
# variables:
Authoriz_Tokens = []
Data_Views = []
Data_Tables = []
Flag = True
# variables for connect to Postgre fo IUM :
Database = '****'
User = '****'
Password = '****'
Host = '****'
Port = '5432'
Table_Name = '****'

Flag, Authoriz_Tokens, Data_Views, Data_Tables = receiving_source_data(Flag, Database, User, Password, Host, Port, Table_Name)
# в случае успешного доступа к БД аттрибут Flag остается с первично заданным значением True
if Flag:
    # цикл по перебору строк подключения из БД (токен, витрина, таблица)
    for Authoriz_Token, Data_View, Data_Table in zip(Authoriz_Tokens, Data_Views, Data_Tables):
        File_Name = f"{Data_View}_{Data_Table}"
        Table_Name = f"{Data_View}__{Data_Table}"
        Flag = create_bash_script(Flag, Authoriz_Token, Data_View, Data_Table, File_Name)
        if Flag:
            Flag = start_bash_script(Flag, File_Name)
            # после отработки POST-запроса (не зависимо от результата) sh-файл будет удален
            os.remove(f"{File_Name}.sh")
        if Flag:
            transfer_to_IUM(Flag, Database, User, Password, Host, Port, Table_Name, File_Name)
            # удаление csv-файла после копирования из него данных в Postgre
            os.remove(f"{File_Name}.csv")