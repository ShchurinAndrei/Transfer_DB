#!/bin/bash

filename="Token_View_Table.txt"
delimiter=","
words=()

while IFS= read -r line; do
  lines+=($(echo "$line" | awk '{print $0}'))
done < "Token_View_Table.txt"

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
  python3 transfer_db_PostgreSQL.py
done

