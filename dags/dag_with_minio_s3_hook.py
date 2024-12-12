from airflow.providers.amazon.aws.hooks.s3 import S3Hook

cid = "minio_s3"                       # the airflow connection id
b_url = 's3://airflow/'   # remote_base_log_folder

hook = S3Hook(cid, transfer_config_args={
    'use_threads': False,
})

parsed_loc = hook.parse_s3_url(b_url)[0]
# 'airflow-logs-dev'
b, k = hook.parse_s3_url(b_url)
# b == 'airflow-logs-dev', k == 'logs'
hook.get_s3_bucket_key('airflow-logs-dev', 'logs', b, k)
# ('airflow-logs-dev', 'logs')

hook.check_for_bucket(parsed_loc)  # parsed_loc == 'airflow-logs-dev'
# [2022-10-20T16:28:15.808+0000] {base.py:71} INFO - Using connection ID 'amazon_s3' for task execution.
# [2022-10-20T16:28:15.868+0000] {connection_wrapper.py:306} INFO -
#       AWS Connection (conn_id='amazon_s3', conn_type='aws') credentials retrieved from extra.
# True
hook.check_for_bucket(b)
# True
hook.check_for_bucket(k)
# [2022-10-20T16:31:37.605+0000] {s3.py:223} ERROR - Bucket "logs" does not exist
# False
# Expected - k is a key, not a bucket

print(hook.list_keys(b))
