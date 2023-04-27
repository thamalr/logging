print('Loading Snowflake Connection Module')
import snowflake.connector
import boto3
import json

def get_secret(secret_name,region_name):
    """
    Input :
        secret_name : Name of the secret key
        region_name : Name of region
        
    Objective : Returns the snowflake secret key values.
        
    Output : Return secret key
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
        )
    secret = None

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
       
    return secret

def createSnowflakeConnection(snowflake_secret):
    """
    Input :
        snowflake_secret : dict object consisting of secret_name and region to create the connection.
        
    Objective : Creates the connection to snowflake using the secret name and region.
        
    Output : snowflake connector (Snowflake object which is used to connect to snowflake warehouses and databases.)        
    """
    data = get_secret(snowflake_secret['secret_name'],snowflake_secret['region'])
    
    if data is not None:
        data = json.loads(data);
        snuser = data.get('snowflake_user')
        password = data.get('snowflake_password')
        account = data.get('snowflake_account')
        wh = data.get('snowflake_warehouse')
        db = data.get('snowflake_database')
        

        conn = snowflake.connector.connect(
            user=snuser,
            password=password,
            account=account,
            warehouse=wh,
            database=db
        )
    else:
        print("snowflake Secret manager return None.")
    return conn

