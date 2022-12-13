from elasticsearch import Elasticsearch, helpers
import elasticsearch
import math
import pandas as pd
import json
from pymongo import MongoClient


class ESClient(Elasticsearch):
    '''Class for accessing Elasticsearch'''

    def __init__(self, path_to_profile):
        '''Authenticate access to Elasticsearch using the profile credentials, saved
        in a JSON file in the folder auth'''
        with open(path_to_profile) as f:
            self.profile = json.load(f)
            if len(self.profile['auth']) > 0:
                Elasticsearch.__init__(self,
                                       self.profile['host'],
                                       ca_certs=self.profile['auth']['ca_certs'],
                                       timeout=60,
                                       http_auth=(self.profile['auth']['username'], self.profile['auth']['password']))
            else:
                Elasticsearch.__init__(self, self.profile['host'])

    def use_profile(self, path_to_profile):
        '''Use a different profile'''
        self.close()
        with open(path_to_profile) as f:
            self.profile = json.load(f)
            Elasticsearch.__init__(self,
                                   self.profile['auth']['host'],
                                   ca_certs=self.profile['auth']['ca_certs'],
                                   http_auth=(self.profile['auth']['username'], self.profile['auth']['password']))

    #
    # ==================== get_index ====================
    #
    def get_index(self, index_name="*"):
        '''Get the index information
        [in] index_name: index name. Default is all indices
        '''
        return self.indices.get(index=index_name)

    # def get_index_records_cnt( self, index_name ):
    #     '''Get the number of records in an index'''
    #     res = self.count( index=index_name )
    #     if elasticsearch.__version__ < '8.0.0':
    #         return res['count']
    #     return res.body['count']

    #
    # ==================== get_index_records_cnt ====================
    #
    def get_index_records_cnt(self, index_name):
        '''Get the number of records in an index
        [in] index_name: name of the index to get the number of records from
        '''
        res = self.count(index=index_name)
        return res['count']

    #
    # ==================== create_index ====================
    #
    def create_index(self, index_name, use_config=True, body=None):
        '''Create an index'''
        if use_config:
            index_names = [index['index'] for index in self.profile["indices"]]
            print(f'Available index configurations: {index_names}')
            print('Type the name of the index configuration to use:')
            selected_index_name = input()
            if selected_index_name not in index_names:
                print(f'Index configuration {selected_index_name} not found')
                return None
            else:
                index_config = [index for index in self.profile["indices"]
                                if index['index'] == selected_index_name][0]
                print(f'Index configuration details:\n {index_config}')
                print('Do you want to use this configuration? (y/n)')
                if input() == 'y':
                    body = {
                        "settings": index_config['settings'],
                        "mappings": {
                            "properties": index_config['scheme']
                        }
                    }
                    print(
                        f'Creating index {index_name} with configuration {selected_index_name}')
                    return self.indices.create(index=index_name, body=body)
                else:
                    return None
        else:
            return self.indices.create(index=index_name, body=body)

    #
    # ==================== delete_index ====================
    #
    def delete_index(self, index_name):
        '''Delete an index
        [in] index_name: name of the index to delete
        '''
        return self.indices.delete(index=index_name)

    #
    # ==================== ingest_one_record ====================
    #
    def ingest_one_record(self, index_name, record):
        '''Ingest one record
        [in] index_name: name of the index to ingest the record to
        [in] record: record to ingest. The record must be a dict
        '''
        # TODO: check the record is a dict
        return self.index(index=index_name, doc_type='_doc', body=record)

    #
    # ==================== ingest_bulk_from_list ====================
    #
    def ingest_bulk_from_list(self, index_name, records):
        '''Ingest bulk data from a list of records
        [in] index_name: name of the index to ingest the records to
        [in] records: list of records to ingest. Each record must be a dict
        '''
        batch_size = 800
        batch_starts = [*range(0, len(records), batch_size)]
        if batch_starts[-1] != len(records):
            batch_starts.append(len(records))

        for i in range(len(batch_starts)-1):
            batch = records[batch_starts[i]:batch_starts[i+1]]
            print(
                f'Ingesting batch {i+1} of {batch_starts[i+1] - batch_starts[i]} records...')

            if elasticsearch.__version__[0] < 8:
                # create bulk actions
                actions = []
                for record in batch:
                    action = {
                        "_index": index_name,
                        "_type": "_doc",
                        "_source": record
                    }
                    actions.append(action)
                    try:
                        helpers.bulk(self, actions)
                        return True
                    except Exception as e:
                        print(f'Error: {e}')
                        return False
            else:
                # create bulk actions
                actions = []
                for record in batch:
                    action = {"index": {"_index": index_name}}
                    actions.append(action)
                    actions.append(record)
                try:
                    self.bulk(index=index_name, operations=actions)
                    return True
                except Exception as e:
                    print(f'Error: {e}')
                    continue

        return True

    #
    # ==================== ingest_bulk_from_csv ====================
    #
    def ingest_bulk_from_csv(self, index_name, csv_file):
        '''Ingest bulk data from a CSV file
        [in] index_name: name of the index to ingest the records to
        [in] csv_file: path to the CSV file to ingest
        '''
        data = pd.read_csv(csv_file)
        data.fillna('', inplace=True)
        scheme = self.indices.get_mapping(index=index_name)[
            index_name]['mappings']['properties']
        field_names = [field for field in scheme.keys()]
        data_trim = data[field_names]
        records = data_trim.to_dict(orient='records')

        batch_size = 800
        batch_starts = [*range(0, len(records), batch_size)]
        if batch_starts[-1] != len(records):
            batch_starts.append(len(records))

        for i in range(len(batch_starts)-1):
            batch = records[batch_starts[i]:batch_starts[i+1]]
            print(
                f'Ingesting batch {i+1} of {batch_starts[i+1] - batch_starts[i]} records...')

            if elasticsearch.__version__[0] < 8:
                # create bulk actions
                actions = []
                for record in batch:
                    action = {
                        "_index": index_name,
                        "_type": "_doc",
                        "_source": record
                    }
                    actions.append(action)
                    try:
                        helpers.bulk(self, actions)
                        return True
                    except Exception as e:
                        print(f'Error: {e}')
                        return False
            else:
                # create bulk actions
                actions = []
                for record in batch:
                    action = {"index": {"_index": index_name}}
                    actions.append(action)
                    actions.append(record)

                try:
                    self.bulk(index=index_name, operations=actions)
                    return True
                except Exception as e:
                    print(f'Error: {e}')
                    continue
        return True
    
    #
    # ==================== load_queries_from_file ====================
    #
    def load_queries_from_file(self, path_to_file):
        '''Load queries from a file
        [in] path_to_file: path to the file containing the queries. E.g. queries.json
        '''
        try:
            with open(path_to_file) as f:
                self.queries = json.load(f)
            # print( "Queries loaded: \n", self.queries )
            return True
        except Exception as e:
            print(f'Error: {e}')
            self.queries = None
            return False

    #
    # ==================== load_queries_from_json_dict ====================
    #
    def load_queries_from_json_dict(self, index_name, query_dict):
        '''Load queries from a json dict
        [in] index_name: name of the index to query
        [in] query_dict: json dict containing the query body
        '''
        query = {
            "query_id": len(self.queries) + 1,
            "index": index_name,
            "query_setting": {
                "page_size": 20
            },
            "query_body": {
                "query": {
                    "bool": query_dict
                }
            }
        }
        self.queries.append(query)
        return len(self.queries)

    #
    # ==================== query ====================
    #
    def query(self, query_id):
        '''Query an index with the query body read from a file
        Return a tuple of ( total_query_hits, total_pages)
        [in] query_id: id of the query to run
        [ret] ( total_query_hits, total_pages): total number of hits and total number of pages
        '''
        if self.queries is None:
            print('Queries not loaded. Please load the query from json file first!')
            return (0, 0)
        else:
            query = next(x for x in self.queries if x['query_id'] == query_id)
            if query is None:
                print(f'Query {query_id} not found')
                return (0, 0)
            print(query['query_body'])

            try:
                self.query_ret = self.search(
                    index=query['index'], body=query['query_body'])
                total_hits = self.query_ret['hits']['total']['value']
                pages = math.ceil(
                    total_hits / query['query_setting']['page_size'])
                print(
                    f'The query returns {total_hits} hits, splitted in {pages} pages')
                return (total_hits, pages)
            except Exception as e:
                print(f'Error: {e}')
                return (0, 0)

    #
    # ==================== query_page ====================
    #
    def query_page(self, query_id, page_num):
        '''Query a page of an index with the query body read from a file
        Return a tuple of ( total_query_hits, next_exist )
        [in] query_id: id of the query to run
        [in] page_num: page number to query
        [ret] ( total_query_hits, next_exist): total number of hits and whether there is a next page
        '''
        if self.queries is None:
            print('Queries not loaded. Please load the query from json file first!')
            return (0, False)
        else:
            query = next(x for x in self.queries if x['query_id'] == query_id)
            if query is None:
                print(f'Query {query_id} not found')
                return (0, False)
            print(query['query_body'])

            try:
                query_body = query['query_body']
                query_body['from'] = (page_num - 1) * \
                    query['query_setting']['page_size']
                query_body['size'] = query['query_setting']['page_size']
                self.query_ret = self.search(
                    index=query['index'], body=query_body)
                total_hits = self.query_ret['hits']['total']['value']

                if total_hits < query['query_setting']['page_size']:
                    next_exist = False
                    print(
                        f'The query returns {total_hits} hits, cannot query the next page')
                else:
                    next_exist = True
                    print(
                        f'The query returns {total_hits} hits, can query the next page')
                return (total_hits, next_exist)
            except Exception as e:
                print(f'Error: {e}')
                return (0, False)

    #
    # ==================== get_docs ====================
    #
    def get_docs(self, index_name, offset, size):
        '''Get documents from an index
        [in] index_name: name of the index to query
        [in] offset: offset of the documents to get
        [in] size: number of documents to get
        [ret] list of documents
        '''
        try:
            ret = self.search(index=index_name, body={
                              "from": offset, "size": size, "query": {"match_all": {}}})
            if ret:
                return ret['hits']['hits']
            else:
                return None
        except Exception as e:
            print(f'Error: {e}')
            return None

    #
    # ==================== match_filter ====================
    #
    def match_filter(self, index_name, field_name, match_term):
        '''Match query
        [in] index_name: name of the index to query
        [in] field_name: name of the field to match
        [in] match_term: term to match
        [ret] total number of hits
        '''
        # Build query body
        query_body = {
            "query": {
                "bool": {
                    "match": {
                        field_name: match_term
                    }
                }
            }
        }

        # Query
        try:
            self.query_ret = self.search(index=index_name, body=query_body)
            total_hits = self.query_ret['hits']['total']['value']
            return total_hits
        except Exception as e:
            print(f'Error: {e}')
            return 0

    #
    # ==================== term_filter ====================
    #
    def term_filter(self, index_name, field_name, term_term):
        '''Term query
        [in] index_name: name of the index to query
        [in] field_name: name of the field to match
        [in] term_term: term to match
        [ret] total number of hits
        '''
        # Build query body
        query_body = {
            "query": {
                "bool": {
                    "term": {
                        field_name: term_term
                    }
                }
            }
        }

        # Query
        try:
            self.query_ret = self.search(index=index_name, body=query_body)
            total_hits = self.query_ret['hits']['total']['value']
            return total_hits
        except Exception as e:
            print(f'Error: {e}')
            return 0

    
    #
    # ==================== range_filter ====================
    #
    def range_filter(self, index_name, field_name, minimum, maximum):
        '''Range query
        [in] index_name: name of the index to query
        [in] field_name: name of the field to match
        [in] minimum: minimum value of the range
        [in] maximum: maximum value of the range
        [ret] total number of hits
        '''
        # Build query body
        query_body = {
            "query": {
                "bool": {
                    "range": {
                        field_name: {
                            "gte": minimum,
                            "lte": maximum
                        }
                    }
                }
            }
        }

        # Query
        try:
            self.query_ret = self.search(index=index_name, body=query_body)
            total_hits = self.query_ret['hits']['total']['value']
            return total_hits
        except Exception as e:
            print(f'Error: {e}')
            return 0

    
    #
    # ==================== query_strings ====================
    #
    def query_strings( self, index_name: str, field_names, operator: str = 'OR', highlight: bool = True, *phrases ):
        '''Query strings
        Add multiple phrases to search, each phrase is a string, combined by 'OR', 'AND' or 'NOR' operator.
        Can search over multiple fields.
        [in] index_name: index name
        [in] field_names: field names to search. Can be a list of field names. E.g. ['title', 'content']
        [in] operator: 'AND', 'OR', 'NOR'
        [in] highlight: True or False
        [in] phrases: phrases to search. E.g. 'hello world', 'good morning'. Can add multiple phrases
        [ret] total hits

        Example: 
        hits_total = es.query_strings('article', ['title', 'content'], 'OR', True, 'hello world', 'good morning')
        hits = es.get( 'query_ret', {} )
        '''
        # Build query body        
        if len(phrases) == 0:
            print('No phrase')
            return 0

        if operator not in ['AND', 'OR', 'NOR']:
            print('Invalid operator')
            return 0
        
        if operator == 'NOR':
            query_body = {
                "query": {
                    "bool": {
                        "query_string": {
                            "fields": field_names,
                            "query": f"NOT({' '.join( f'({phrase})' for phrase in phrases )})"
                        }
                    }
                }
            }
        else:
            query_body = {
                "query": {
                    "bool": {
                        "query_string": {
                            "fields": field_names,
                            "default_operator": operator,
                            "query": ' '.join( f'({phrase})' for phrase in phrases )
                        }
                    }
                }
            }
        
        if highlight:
            query_body['highlight'] = {
                "fields": {
                    "*": {}
                }
            }

        # Query
        try:
            self.query_ret = self.search(index = index_name, body = query_body)
            total_hits = self.query_ret['hits']['total']['value']
            return total_hits
        except Exception as e:
            print(f'Error: {e}')
            return 0