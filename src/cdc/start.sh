bash run.sh register_connector ./configs/postgresql-cdc.json
python3 -m create_table
python3 -m insert_data
