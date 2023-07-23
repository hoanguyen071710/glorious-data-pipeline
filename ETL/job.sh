python3 ./prod_to_s3_load.py >> ./log/prod_to_s3_load_log.txt 2>&1
python3 ./s3_to_dw_load.py >> ./log/s3_to_dw_load_log.txt 2>&1