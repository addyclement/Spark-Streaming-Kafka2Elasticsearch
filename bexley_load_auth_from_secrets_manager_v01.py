# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

import boto3,json,sys
from botocore.exceptions import ClientError

# refer to this for try catch
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/secrets-manager.html

def get_secret_from_sm(secret_name, region_name="eu-west-2"):

    # secret_name = "/analytics/rds/mysql_readonly"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(get_secret_value_response['SecretString'])
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # raise e
        print("An error occured retreiving the required secret ",e)

        sys.exit(1)

    # Your code goes here.
    return secret



