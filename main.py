"""
Sample code to demonstrate how to use boto3 to delete
all the rows in a DynamoDB table.
"""

import boto3


AWS_REGION = "ap-southeast-2"
TABLE_NAME = "my_table"
BATCH_DELETE_SIZE = 25

ddb_client = boto3.resource("dynamodb")


def create_table(table_name):
    """
    Create a DynamoDB table.
    """
    return ddb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {
                "AttributeName": "id",
                "KeyType": "HASH",
            },
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "id",
                "AttributeType": "S",
            },
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
    )


def delete_table(table_name):
    """
    Delete a DynamoDB table.
    """
    ddb_client.Table(table_name).delete()


def scan_table(ddb_table, batch_size):
    """
    Scan a DynamoDB table.
    """
    response = ddb_table.scan(Limit=batch_size)
    return response.get("Items", [])


def delete_items(ddb_table, items):
    """
    Delete items from a DynamoDB table.
    """
    with ddb_table.batch_writer() as ddb_batch:
        for data in items:
            ddb_batch.delete_item(Key={"id": data["id"]})


def delete_all_items(table_name, batch_size=25):
    """
    Delete all items in a DynamoDB table.
    """
    ddb_table = ddb_client.Table(table_name)
    items = scan_table(ddb_table, batch_size)

    while items:
        print(f"Deleting up to {batch_size} items")
        delete_items(ddb_table, items)
        items = scan_table(ddb_table, batch_size)


if __name__ == "__main__":
    try:
        print(f"Creating table {TABLE_NAME}")
        table = create_table(TABLE_NAME)
    except Exception as e:
        if e.__class__.__name__ == 'ResourceInUseException':
            print(f"Table {TABLE_NAME} already exists, skipping creation.")
            table = ddb_client.Table(TABLE_NAME)
        else:
            raise e

    # wait for table to be created
    print(f"Waiting for table {TABLE_NAME} to be created")
    table.wait_until_exists()

    # batch put 1000 items
    print(f"Batch writing all items to table {TABLE_NAME}")
    for i in range(10):
        write_items = []
        for j in range(25):
            write_items.append({
                "id": f"{i}-{j}",
                "name": f"item-{i}-{j}",
            })
        with table.batch_writer() as batch:
            print(f"Writing {len(write_items)} items")
            for item in write_items:
                batch.put_item(Item=item)

    # delete all items
    print(f"Deleting all items in table {TABLE_NAME}")
    delete_all_items(TABLE_NAME, BATCH_DELETE_SIZE)

    try:
        print(f"Deleting table {TABLE_NAME}")
        delete_table(TABLE_NAME)
    except Exception as e:
        # ResourceInUseException
        if e.__class__.__name__ == 'ResourceInUseException':
            print(f"Table {TABLE_NAME} is in use, try again later.")

        if e.__class__.__name__ == 'ResourceNotFoundException':
            print(f"Table {TABLE_NAME} does not exist, skipping deletion.")
        else:
            raise e
