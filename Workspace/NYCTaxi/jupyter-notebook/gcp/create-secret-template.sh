databricks secrets create-scope databricks-warehouse

databricks secrets put-secret --json '{
  "scope": "databricks-warehouse",
  "key": "sql-host",
  "string_value": <your_value>
}'

databricks secrets put-secret --json '{
  "scope": "databricks-warehouse",
  "key": "sql-token",
  "string_value": <your_value>
}'

databricks secrets put-secret --json '{
  "scope": "databricks-warehouse",
  "key": "warehouseid",
  "string_value": <your_value>
}'

databricks secrets create-scope databricks-bigquery

(cat << EOF 
{<your_secret_key_json>}
EOF
) | databricks secrets put-secret databricks-bigquery databricks-bq-sa