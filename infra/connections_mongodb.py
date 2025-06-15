import atexit
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.cursor import Cursor
from abc import ABC
from typing import List, Dict, Any


class MongoDBConnector(ABC):
    """MongoDB connector abstract class"""

    def __init__(self, connection_string: str, database_name: str) -> None:
        self.client = MongoClient(connection_string)
        self.database = self.client[database_name]
        atexit.register(self.close)

    def create_index(self, collection_name: str, field_name: str) -> None:
        """Create index on field if it doesn't already exist"""
        collection = self.database[collection_name]
        
        existing_indexes = collection.list_indexes()
        for index in existing_indexes:
            if field_name in index.get('key', {}):
                return
        
        collection.create_index(field_name)

    def num_docs(self, collection_name: str) -> int:
        """Get estimated document count for collection"""
        return self.database[collection_name].estimated_document_count()

    def drop_collection(self, collection_name: str) -> None:
        """Drop collection"""
        print(f"Dropping collection {collection_name}")
        self.database[collection_name].drop()

    def close(self) -> None:
        """Close MongoDB connection"""
        print("Closing MongoDB connection")
        self.client.close()


class MongoDBJobDB(MongoDBConnector):
    """MongoDB job database operations"""
    
    def get_documents_batch(self, collection_name: str, query: Dict[str, Any]) -> Cursor:
        """Get documents batch by query"""
        return self.database[collection_name].find(query)
    
    def bulk_write(self, collection_name: str, batch: List[Any]):
        """Execute bulk write operations"""
        if not batch:
            return None
        
        return self.database[collection_name].bulk_write(batch)

    def batch_insert(self, collection_name: str, documents: List[Dict[str, Any]]):
        """Insert multiple documents"""
        if not documents:
            return None
            
        batch = [InsertOne(doc) for doc in documents]
        return self.bulk_write(collection_name, batch)

    def batch_update(
        self, 
        collection_name: str, 
        queries: List[Dict[str, Any]], 
        updates: List[Dict[str, Any]],
        upsert: bool = True
    ):
        """Update multiple documents"""
        if not queries or not updates or len(queries) != len(updates):
            return None
            
        batch = [
            UpdateOne(query, update, upsert=upsert) 
            for query, update in zip(queries, updates)
        ]
        return self.bulk_write(collection_name, batch)
    
    def upsert_documents(self, collection_name: str, documents: List[Dict[str, Any]], key_field: str):
        """Insert or replace documents based on key field to avoid duplicates"""
        if not documents:
            return None
            
        batch = [
            UpdateOne(
                {key_field: doc[key_field]}, 
                {"$set": doc}, 
                upsert=True
            ) 
            for doc in documents if key_field in doc
        ]
        return self.bulk_write(collection_name, batch)
    
    def batch_delete_by_query(self, collection_name: str, query: Dict[str, Any]):
        """Delete documents by query using delete_many"""
        if not query:
            return None
        
        return self.database[collection_name].delete_many(query)