from typing import Dict,Tuple
from abc import abstractmethod, ABC
from singer.metadata import get_standard_metadata
from singer import get_logger,get_bookmark,metrics,write_record

LOGGER = get_logger()


class BaseStream(ABC):
    """
    Base class representing generic stream methods and meta-attributes
    """
    @property
    def stream(self) -> str:
        """
        TODO: Documentation
        """
        return None

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """
        The unique identifier for the stream. This is allowed to be different from the name of the stream in order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """
        Defines the sync mode of a stream
        """

    @property
    @abstractmethod
    def replication_key(self) -> str:
        """
        Defines the replication key for incremental sync mode of a stream
        """

    @property
    @abstractmethod
    def valid_replication_keys(self) -> Tuple[str, str]:
        """
        Defines the replication key for incremental sync mode of a stream
        """

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """
        Defines the sync mode of a stream
        """


    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """
        List of key properties for stream
        """

    @property
    def selected_by_default(self) -> bool:
        """
        Indicates if a node in the schema should be replicated if a user has not expressed any opinion on whether or not to replicate it.
        """
        return False

    @property
    @abstractmethod
    def url_endpoint(self) -> str:
        """
        Defines the HTTP endpoint for the stream 
        """

    @abstractmethod
    def get_records(self):
        """
        TODO: Add Documentation
        """

    @abstractmethod
    def sync(self,*args,**kwargs) ->Dict:
        """
        TODO: Add Documentation
        """

    def __init__(self, client=None) -> None:
        self.client = client

    @classmethod
    def get_metadata(cls, schema) -> Dict[str, str]:
        """
        Returns a `dict` for generating stream metadata
        """
        metadata = get_standard_metadata(**{
                "schema": schema,
                "key_properties": cls.key_properties,
                "valid_replication_keys": cls.valid_replication_keys,
                "replication_method": cls.replication_method
                or cls.forced_replication_method,
            }
        )
        if cls.replication_key is not None:
            meta = metadata[0]["metadata"]
            meta.update({"replication-key":cls.replication_key})
            metadata[0]["metadata"] = meta
        return metadata


class IncremetalStream(BaseStream):
    """
    Base Class for Incremental Stream
    """

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    config_start_key = None

    def get_bookmark(self,state :dict,) ->int:
        """
        A wrapper for singer.get_bookmark to deal with compatibility for bookmark values or start values.
        """
        return get_bookmark(state, self.tap_stream_id, self.replication_key, self.client.config.get(self.config_start_key,False))
        
    
    @abstractmethod
    def filter_record(self,record :Dict,state :Dict) ->bool:
        """
        Returns boolean if a record should be written
        """

    def sync(self, state: dict, schema: dict, stream_metadata: dict, transformer):
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                if self.filter_record(record,state):
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
        return state     

class FullTableStream(BaseStream):
    """
    Base Class for Incremental Stream
    """

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_key = None

    def sync(self,state,schema,stream_metadata,transformer):
        #LOGGER.info("sync called from %s", self.__class__)
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()
        return state 