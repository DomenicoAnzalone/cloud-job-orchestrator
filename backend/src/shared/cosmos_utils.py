import os
from azure.cosmos import CosmosClient

_cosmos_container = None

def get_cosmos_container():
    global _cosmos_container
    if _cosmos_container is not None:
        return _cosmos_container

    endpoint = os.environ["COSMOS_ENDPOINT"]
    key = os.environ["COSMOS_KEY"]
    db_name = os.environ.get("COSMOS_DB", "cjo")
    container_name = os.environ.get("COSMOS_CONTAINER", "jobs")

    client = CosmosClient(endpoint, credential=key)
    db = client.get_database_client(db_name)
    _cosmos_container = db.get_container_client(container_name)
    return _cosmos_container