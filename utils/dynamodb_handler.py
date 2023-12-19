# Author: Dulanga Heshan
# Date Created: YYYY-MM-DD
# Date Updated: YYYY-MM-DD
# import boto3
from datetime import datetime

from boto3 import client, resource
from botocore.exceptions import NoCredentialsError
from decouple import config

print(config("AWS_ACCESS_KEY_ID"))

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")
REGION_NAME = config("REGION_NAME")
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime

# DynamoDB client and resource
client = boto3.client(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

resource = boto3.resource(
    'dynamodb',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)


def create_repositories_table():
    table_name = 'Repositories'
    try:
        # Check if the table exists
        RepositoriesTable = resource.Table(table_name)
        RepositoriesTable.table_status  # This line will raise an exception if the table doesn't exist

        print(f"Table '{table_name}' already exists.")
    except RepositoriesTable.meta.client.exceptions.ResourceNotFoundException:
        # Table does not exist, create it
        client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'day_id',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'day_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'id',
                    'KeyType': 'RANGE'
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            Tags=[
                {
                    'Key': 'test-resource',
                    'Value': 'dynamodb-test'
                }
            ]
        )

        print(f"Table '{table_name}' created successfully.")


# Function to check if a repository already exists
def repository_exists(day_id, repository_id):
    response = RepositoriesTable.get_item(
        Key={
            'day_id': day_id,
            'id': repository_id
        }
    )
    return 'Item' in response


# Function to add or update a repository
def add_or_update_repository(github_repository):
    day_id = int(datetime.utcnow().strftime('%Y%m%d'))
    repository_id = github_repository['id']

    if repository_exists(day_id, repository_id):
        # Repository already exists for the given day_id and repository_id, perform update
        return update_repository(day_id, repository_id, github_repository)
    else:
        # Repository doesn't exist, perform insert
        return add_repository(github_repository)


# Function to add a repository
def add_repository(github_repository):
    # Extracting relevant attributes from GitHub API response
    repository_data = {
        'day_id': int(datetime.utcnow().strftime('%Y%m%d')),
        'id': github_repository['id'],
        'name': github_repository['name'],
        'full_name': github_repository['full_name'],
        'description': github_repository['description'],
        'language': github_repository['language'],
        "created_at": github_repository['created_at'],
        "updated_at": github_repository['updated_at'],
        "pushed_at": github_repository['pushed_at'],
        'stargazers_count': github_repository['stargazers_count'],
        'watchers_count': github_repository['watchers_count'],
        'has_issues': github_repository['has_issues'],
        'has_projects': github_repository['has_projects'],
        'has_downloads': github_repository['has_downloads'],
        'has_discussions': github_repository['has_discussions'],
        'forks_count': github_repository['forks_count'],
        'archived': github_repository['archived'],
        'disabled': github_repository['disabled'],
        'open_issues_count': github_repository['open_issues_count'],
        'allow_forking': github_repository['allow_forking'],
        'topics': github_repository['topics'],
        'visibility': github_repository['visibility'],
        'forks': github_repository['forks'],
        'open_issues': github_repository['open_issues'],
        'watchers': github_repository['watchers'],
        'default_branch': github_repository['default_branch']
    }

    response = RepositoriesTable.put_item(Item=repository_data)
    return response


# Function to update a repository
def update_repository(day_id, repository_id, data):
    update_expression = 'SET ' + ', '.join([f'#key_{key} = :{key}' for key in data.keys() if key != 'id'])
    expression_attribute_names = {f'#key_{key}': key for key in data.keys() if key != 'id'}
    expression_attribute_values = {f':{key}': value for key, value in data.items() if key != 'id'}

    response = RepositoriesTable.update_item(
        Key={
            'day_id': day_id,
            'id': repository_id
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="ALL_NEW"
    )

    return response


# Function to get a repository by day_id and id
def get_repository(day_id, repository_id):
    response = RepositoriesTable.get_item(
        Key={
            'day_id': day_id,
            'id': repository_id
        },
        AttributesToGet=['name', 'full_name', 'description', 'language',
                         'stargazers_count', 'watchers_count', 'has_issues',
                         'has_projects', 'has_downloads', 'has_wiki', 'has_pages',
                         'has_discussions', 'forks_count', 'archived',
                         'disabled', 'open_issues_count', 'allow_forking', 'is_template',
                         'web_commit_signoff_required', 'topics', 'visibility', 'forks',
                         'open_issues', 'watchers', 'default_branch']
    )

    return response


# Function to delete a repository by day_id and id
def delete_repository(day_id, repository_id):
    response = RepositoriesTable.delete_item(
        Key={
            'day_id': day_id,
            'id': repository_id
        }
    )

    return response


# Function to create the "Users" table
def create_users_table():
    table_name = 'Users'

    try:
        # Check if the table exists
        UsersTable = resource.Table(table_name)
        UsersTable.table_status  # This line will raise an exception if the table doesn't exist

        print(f"Table '{table_name}' already exists.")
    except UsersTable.meta.client.exceptions.ResourceNotFoundException:
        # Table does not exist, create it
        client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'repository',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'id',
                    'AttributeType': 'N'
                }
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'repository',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'id',
                    'KeyType': 'RANGE'
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            Tags=[
                {
                    'Key': 'test-resource',
                    'Value': 'dynamodb-test'
                }
            ]
        )

        print(f"Table '{table_name}' created successfully.")


# Table references
RepositoriesTable = resource.Table('Repositories')
UsersTable = resource.Table('Users')
EventsTable = resource.Table('Events')

# Function to check if a user already exists
def user_exists(repository, user_id):
    response = UsersTable.get_item(
        Key={
            'repository': repository,
            'id': user_id
        }
    )
    return 'Item' in response


# Function to add or update a user
def add_or_update_user(user, repo_name):
    repository = repo_name
    user_id = user['id']

    user_data = {
        'repository': repository,
        'id': user_id,
        'login': user['login'],
        'node_id': user['node_id'],
        'avatar_id': user.get('avatar_id', ''),  # Omitted URL
        'gravatar_id': user.get('gravatar_id', ''),
        'type': user['type'],
        'site_admin': user['site_admin'],
        'contributions': user['contributions']
    }

    if user_exists(repository, user_id):
        return update_user(repository, user_id, user_data)
    else:
        return add_user(user_data)


# Function to add a user
def add_user(user_data):
    response = UsersTable.put_item(Item=user_data)
    return response


# Function to update a user
def update_user(repository, user_id, user_data):
    update_expression = 'SET ' + ', '.join([f'{key} = :{key}' for key in user_data.keys()])
    expression_attribute_values = {f':{key}': value for key, value in user_data.items()}

    response = UsersTable.update_item(
        Key={
            'repository': repository,
            'id': user_id
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="ALL_NEW"
    )

    return response


# Function to save a user
def save_user(user, repo_name):
    add_or_update_user(user, repo_name)


def table_exists(table_name):
    existing_tables = client.list_tables()['TableNames']
    return table_name in existing_tables


def create_events_table():
    table_name = 'Events'

    try:
        if not table_exists(table_name):
            # Define table schema
            table = client.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'day_id', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'repository_name', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'day_id', 'AttributeType': 'S'},
                    {'AttributeName': 'repository_name', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )

            # Wait for the table to be created
            client.get_waiter('table_exists').wait(TableName=table_name)
            print(f'Table {table_name} created successfully!')
        else:
            print(f'Table {table_name} already exists.')

    except client.exceptions.ResourceInUseException:
        print(f'Table {table_name} already exists.')


def add_event(details):
    try:
        # Add item to the table
        EventsTable.put_item(Item=details)
        print('Item added successfully!')

    except NoCredentialsError:
        print('Credentials not available.')


def get_events(partition_key, sort_key):
    try:
        response = EventsTable.get_item(
            Key={
                'day_id': partition_key,
                'repository_name': sort_key
            }
        )

        item = response.get('Item')
        if item:
            print('Item found:', item)
        else:
            print('Item not found.')

    except NoCredentialsError:
        print('Credentials not available.')
