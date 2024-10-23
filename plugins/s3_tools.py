import re
from airflow.models import Variable
import boto3


def get_s3_credentials(var_name):
    vars = Variable.get(var_name, deserialize_json=True)
    return vars["s3_url"], vars["s3_access_key_id"], vars["s3_secret_access_key"]


def get_s3_client(var_name):
    s3_url, s3_id, s3_secret = get_s3_credentials(var_name)
    s3_client = boto3.client(
        service_name="s3",
        endpoint_url=s3_url,
        aws_access_key_id=s3_id,
        aws_secret_access_key=s3_secret,
    )
    return s3_client


def s3_get_all(s3_client, bucket, prefix=""):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    result = []
    for page in pages:
        if not "Contents" in page.keys():
            return result

        for obj in page["Contents"]:
            result.append(obj)
    return result


def s3_delete_all(s3_client, bucket, prefix=""):
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        if not "Contents" in page.keys():
            break
        for_deletion = []
        for obj in page["Contents"]:
            for_deletion.append({"Key": obj["Key"]})
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": for_deletion})


def clickhouse_to_parquet_data_types_convertion(column_types):
    ch_to_parquet_patterns = {
        "Int8": r"Enum8\(.*?\)",
        "Timestamp": r"DateTime64\(.*?\)",
    }
    ch_to_parquet_replacements = {
        "DateTime": "Timestamp",
        "UUID": "String",
    }
    result = []
    for val in column_types:
        for replacement, pattern in ch_to_parquet_patterns.items():
            val = re.sub(pattern, replacement, val)

        for target, replacement in ch_to_parquet_replacements.items():
            val = val.replace(target, replacement)

        result.append(val)

    return result


def compose_columns_types_str_for_parquet(columns_defs):
    names, types = list(zip(*columns_defs))

    columns_string = ", ".join(v for v in names)
    converted_types = clickhouse_to_parquet_data_types_convertion(types)
    columns_types_string = ", ".join(f"{col} {tp}" for col, tp in zip(names, converted_types))

    return (columns_string, columns_types_string)
