#!/bin/bash

hostname="refdb-test.database-dev-pg.cloud.vimpelcom.ru"
username="ium"
database_name="iumdb"
QUERY="SELECT * FROM test_token_view_table;"
RESULT=$(psql -h $hostname -U $username -d $database_name -c "$QUERY")

# Обработка строк (если нужно)
COLUMNS=$(echo "$RESULT" | head -n 1 | tr ' ' '\n')
for col in $COLUMNS; do
    echo "Column: $col"
done

# Разделение результатов по строкам
IFS=$'\n\n' read -ra ADDR <<< "$RESULT"
for line in "${ADDR[@]}"; do
    if [ -z "$line" ]; then
        continue
    fi
    echo "Строка:"
    IFS=' ' read -ra ROW <<< "$line"
    for ((i=0; i<${#ROW[@]}; i++)); do
        echo "  ${COLUMNS[$i]}: ${ROW[i]}"
    done
    echo ""
done

delimiter=","
words=()


for i in "${lines[@]}"; do
  IFS=',' read -ra words <<< "$i"
  Token="${words[0]}"
  VIEW="${words[1]}"
  TABLE="${words[2]}"
  curl --insecure -X POST 'https://****/API/TDS/v1/SQL_CSV/:FileName/:SaveOption?delimeter=;&charset=UTF-8_BOM' \
  -H 'Content-Type: application/json' \
  -H "Authorization-Token: ${Token}" \
  -d "SELECT * FROM \"${VIEW}\".\"${TABLE}\"" > ${VIEW}_${TABLE}.csv
  . transfer_db_venv/bin/activate
  export View="${VIEW}"
  export Table="${TABLE}"
  python3 transfer_db_MySQL.py
done

