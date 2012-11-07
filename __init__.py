"""
This module is a database abstraction layer for accessing and manipulating a MongoDB instance.

"""
import bson
from pymongo import Connection

conn = None
def init(db_name):
    global conn
    # Initialise the connection
    conn = Connection()[db_name]
    

class Document(object):
    """
    Base Document class.
    
    The data dict holds all fields of the instance that correspond to real data in the database.
    
    """    
    def __init__(self, **kwargs):
        self._data = {}
        self.set_properties(kwargs)
        self._name = type(self).__name__.lower()
        self.__after_init__()
        
    def __after_init__(self):
        """
        Hook for any processing after the init method has been run.
        
        """
        pass
            
    def __setattr__(self, k, v):
        """
        Any time a non-private attribute is directly set on a Document instance, we store it in the
        data dict.
        
        """
        self.__dict__.update({k:v})
        if not k.startswith('_'):
            self._data.update({k:v})
            
    def delete(self):
        """
        Delete this object from the database.
        
        """
        id = self._id
        conn[self._name].remove({'_id' : id})
            
    def get_id(self, format=True):
        """
        Get the database ID (as a string if format param is True).
        
        """
        if format:
            return str(self._id)
        else:
            return self._id
        
    def set_properties(self, props):
        """
        Update the properties of this object with the specified props dict.
        
        """
        for k,v in props.items():
            self.__setattr__(k, v)
        if '_id' in props.keys():
            # Some special handling to set the ID directly on the instance
            self._id = props['_id']
            
    def get_properties(self):
        """
        Get all the properties of this object that represent database values, as a dict.
        
        """
        return self._data
     
    def save(self, blocking=False):
        """
        Save this object to the database via insert or update. By default writes are asnychronous.
        The blocking argument allows writes to be made synchronous.
        
        """
        if hasattr(self, 'before_save'):
            self.before_save()
        if not hasattr(self, '_id'):
            # Insert
            self._id = conn[self._name].insert(self._data, safe=blocking)
        else:
             # Update
            conn[self._name].update({'_id' : self._id}, self._data, safe=blocking)
        
        return self._id
    
    @classmethod
    def all(cls):
        """
        Get all instances of this Document type from the database.
        
        """
        return [cls._objectify(d) for d in cls._find()]
     
    @classmethod   
    def count(cls, query=None):
        """
        Get a count of instances of this object in the database.
        
        """
        return cls._find(query).count()
        
    @classmethod
    def ensure_index(cls, index):
        """
        Make sure the specified index exists for this collection.
        
        """
        conn[cls.__name__.lower()].ensure_index(index)
        
    @classmethod
    def find(cls, query):
        """
        Get all matching instances of this Document from the database for the specified query.
        
        """
        return [cls._objectify(d) for d in cls._find(query)]
        
    @classmethod
    def find_one(cls, query=None):
        """
        Get a single matching instance of this Document from the database for the specified query.
        
        """
        return cls._objectify(cls._find_one(query))
        
    @classmethod
    def get(cls, id):
        """
        Get a single matching instance of this Document from the database for the specified ID
        string.
        
        """
        result = conn[cls.__name__.lower()].find_one({'_id' : bson.ObjectId(id)})

        if result is not None:
            return cls._objectify(result)
        return None
        
    @classmethod    
    def _map_reduce(cls, m, r, query=None):
        """
        Perform a map reduce operation on the database for this Document type.
        
        """
        return conn[cls.__name__.lower()].map_reduce(m, r, query=query)     
        
    @classmethod
    def _find(cls, query_obj=None):
        """
        Perform a query for instances of this Document type in the database.
        
        """
        return conn[cls.__name__.lower()].find(query_obj)
        
    @classmethod
    def _find_one(cls, query_obj=None):
        """
        Perform a query for a single instance of this Document type in the database.
        
        """
        return conn[cls.__name__.lower()].find_one(query_obj)
        
    @classmethod
    def _objectify(cls, data):
        """
        Create a Document instance from a dict of data.
        
        """
        instance = cls()
        instance.set_properties(data)
        return instance