import boto3
import base64
import json
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region_name = 'us-east-1'

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    trip_details_table = dynamodb.Table('trip_details')
    glue = boto3.client('glue', region_name=region_name)

    record_count = 0
    error_count = 0
    updated_any = False

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)

            # Check if trip_id exists in DynamoDB
            response = trip_details_table.get_item(Key={'trip_id': data_item['trip_id']})
            if 'Item' not in response:
                logger.info(f"Skipping trip_id {data_item['trip_id']} as it does not exist in trip_details.")
                continue

            # Convert numerical values to Decimal
            for field in ['fare_amount', 'tip_amount', 'total_amount']:
                if field in data_item:
                    data_item[field] = Decimal(str(data_item[field]))

            # Mark this trip as finished
            data_item['status'] = 'completed'

            # Upsert the data into DynamoDB
            # Use ExpressionAttributeNames (#k) for every field, since some field names
            # (e.g. 'status') are reserved keywords in DynamoDB and can't be used directly.
            update_fields = [k for k in data_item if k != 'trip_id']
            update_expression = "SET " + ", ".join([f"#{k} = :{k}" for k in update_fields])
            expression_attribute_names = {f"#{k}": k for k in update_fields}
            expression_attribute_values = {f":{k}": v for k, v in data_item.items() if k != 'trip_id'}
            update_response = trip_details_table.update_item(
                Key={'trip_id': data_item['trip_id']},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW"
            )

            if update_response['Attributes']:
                updated_any = True

            record_count += 1
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            error_count += 1

    # Trigger the Glue job at most ONCE per Lambda invocation (i.e. once per batch),
    # not once per record, to avoid ConcurrentRunsExceededException.
    glue_triggered_count = 0
    if updated_any:
        try:
            glue_response = glue.start_job_run(JobName='process_completed_trips')
            logger.info(f"Triggered Glue job: {glue_response['JobRunId']}")
            glue_triggered_count = 1
        except glue.exceptions.ConcurrentRunsExceededException:
            logger.info("Glue job 'process_completed_trips' is already running; skipping trigger for this batch.")
        except Exception as e:
            logger.error(f"Error triggering Glue job: {e}")

    logger.info(f'Updated {record_count} trips with {error_count} errors. Triggered Glue job {glue_triggered_count} times.')
    return {
        'statusCode': 200,
        'body': json.dumps(f'Updated {record_count} trips with {error_count} errors. Triggered Glue job {glue_triggered_count} times.')
    }