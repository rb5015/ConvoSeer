#!/usr/bin/env python3
import os
from pymongo import MongoClient


def main() -> int:
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB", "my_database")
    coll_name = os.getenv("MONGODB_COLLECTION", "call_chunks")
    dims = int(os.getenv("EMBEDDING_DIMS", "1024"))
    if not uri:
        print("MONGODB_URI is required in environment")
        return 1
    client = MongoClient(uri)
    db = client[db_name]
    # Atlas Search vector index creation
    result = db.command({
        "createSearchIndexes": coll_name,
        "indexes": [
            {
                "name": "embedding_index",
                "definition": {
                    "fields": [
                        {
                            "type": "vector",
                            "path": "embedding",
                            "numDimensions": dims,
                            "similarity": "cosine"
                        }
                    ]
                }
            }
        ]
    })
    print("Index creation result:", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


