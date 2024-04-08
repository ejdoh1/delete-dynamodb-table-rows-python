# Batch Delete Items from DynamoDB Table with Python

This sample Python script demonstrates how to delete all items from a DynamoDB table using the `batch_writer` method.

Batch size can be adjusted to optimize performance. The default batch size is 25 items.

```python
import boto3
import os

ddb_client = boto3.resource("dynamodb")
table_name = os.environ.get("TABLE_NAME", "my_table")


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


def delete_all_items(ddb_table_name, batch_size=25):
    """
    Delete all items in a DynamoDB table.
    """
    ddb_table = ddb_client.Table(ddb_table_name)
    items = scan_table(ddb_table, batch_size)

    while items:
        print(f"Deleting up to {batch_size} items")
        delete_items(ddb_table, items)
        items = scan_table(ddb_table, batch_size)


if __name__ == "__main__":
    delete_all_items(table_name)

```

See main.py for a full example.
