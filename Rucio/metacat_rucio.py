from metacat.webapi import MetaCatClient

class MetaCatRucioPlugin(object):
    
    def __init__(self, client):
        self.Client = client
        
    def get_metadata(self, scope, name, session=None):
        info = self.Cient.get_file_info(name=f"{scope}:{name}")
        return info.Metadata

    def set_metadata(self, scope, name, key, value, recursive=False, session=None):
        self.Client.update_file_meta(
            {key:value},
            names=[f"{scope}:{name}"]
        )
        

    @transactional_session
    def set_metadata_bulk(self, scope, name, meta, recursive=False, session=None):
        self.Client.update_file_meta(
            meta,
            names=[f"{scope}:{name}"]
        )

    @abstractmethod
    def delete_metadata(self, scope, name, key, session=None):
        meta = self.get_metadata(scope, name)
        try:    del meta[key]
        except KeyError:    return
        self.Client.update_file_meta(
            meta,
            names=[f"{scope}:{name}"],
            mode="replace"
        )

    @abstractmethod
    def list_dids(self, scope, filters, type='collection', ignore_case=False, limit=None,
                  offset=None, long=False, recursive=False, session=None):
        
        where_items = []
        for k, v in filters.items():
            if isinstance(v, str):
                where_items.append(f"{k} = '{v}'")
            else:
                where_items.append(f"{k} = {v}")
        where_clause = " and ".join(where_items)
                  
        if type in ("collection", "dataset", "container"):
            if where_clause:    where_clause = " having "+where_clouse
            query = f"datasets {scope}:'%' {where_clause}"
            if recursive:
                query += " with children recursively"
        else:
            if where_clause:    where_clause = " where "+where_clouse
            query = f"files from {scope}:'%'"
            if recursive:
                query += " with children recursively"
            query += where_clouse
        
        if limit is not None:
            query += f" limit {limit}"
            
        results = self.Client.run_query(query)
        return [item["name"] for item in results]
                  
    @abstractmethod
    def manages_key(self, key):
        return True

    