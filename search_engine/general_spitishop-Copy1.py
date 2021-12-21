import pandas as pd
from elasticsearch import Elasticsearch
from greeklish.converter import Converter

#dir_ = ''
dir_ = '/home/spitishop/spitishop_rec_app/'

# create an object for greeklish translation
conv = Converter(max_expansions=1)

# read the spitishop products
df_products = pd.read_pickle(dir_ + 'data/df_products.pkl')

# create a column with the title having changed some letters based on greek_letters_dict
greek_letters_dict = {'ω':'ο'}
df_products['Product_name2'] = df_products.apply(lambda x: x["Product_name"].lower().translate(str.maketrans(greek_letters_dict)), axis=1)
# create a column with the greeklish title
df_products['Product_name_eng'] = df_products.apply(lambda x: conv.convert(x["Product_name2"])[0], axis=1)
# keep only specific columns
df_products = df_products[['Code', 'Price', 'Product_name', 'Url', 'Brand', 'Product_name_eng', 'name', 'img_url', 'freq']]
# drop rows with nans
df_products = df_products.dropna()
# drop duplicates
df_products = df_products.drop_duplicates(subset=['Code'], keep='first')

def connect_elasticsearch():
    _es = None
    #_es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    _es = Elasticsearch()
    if _es.ping():
        print('- Connected')
    else:
        print('- Problem with the connection')
    return _es

es = connect_elasticsearch()

def get_results_spitishop(query, category):

    if query.isdigit():
        #print('- You are searching a product code')
        df_product = df_products[df_products.Code==int(query)]
        
        if len(df_product)>0:
            #print('-- The product code was found')
            results = [[{'Brand': df_product.iloc[0]['Brand'],
                                          'Category': df_product.iloc[0]['name'],
                                          'Code': df_product.iloc[0]['Code'],
                                          'Price': df_product.iloc[0]['Price'],
                                          'img_url': df_product.iloc[0]['img_url'],
                                          'title_eng': df_product.iloc[0]['Product_name_eng'],
                                          'Product_name': df_product.iloc[0]['Product_name'],
                                          'Url': df_product.iloc[0]['Url']}]]
            
            return results, 'product_id'
            
    
    # convert the query to lowercase and some letters based on greek_letters_dict
    query = query.lower().translate(str.maketrans(greek_letters_dict))
    # convert it to greeklish
    query = conv.convert(query)[0]
    
    # search_flag defines whether the results are procuced by elasticsearch or based on popularity
    search_flag = 0 
    
    # search the index based on the query    
    if category=='All':
        # search the index based on the query
        results = es.search(index='dimostest1', body = { "from" : 0, "size" : 1000,
            'query':{
                'bool': {
                  'must': [
                                {'match':{'title_eng':{"query" : query,
                                                       "fuzziness": "AUTO:3,5", # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness
                                                       #"fuzziness": 2,
                                                       #"fuzzy_transpositions":False,
                                                        "analyzer": "my_analyzer"

                                                      }}},
                                #{'match': {'Category': category}},
                           ],
#                   "filter": [ 
#                                 {"term":{"Category":category}},
#                             ]
                    
                }
            }
        },                  
                           )
    else:
        results = es.search(index='dimostest1', body = { "from" : 0, "size" : 1000,
            'query':{
                'bool': {
                  'must': [
                                {'match':{'title_eng':{"query" : query,
                                                       "fuzziness": "AUTO:3,5", # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness
                                                       #"fuzziness": 2,
                                                       #"fuzzy_transpositions":False,
                                                        "analyzer": "my_analyzer"

                                                      }}},
                                {'match': {'Category': category}},
                           ],
#                   "filter": [ 
#                                 {"term":{"Category":category}},
#                             ]
                    
                }
            }
        },                  
                           )

    # list of results
    res = results['hits']['hits'][0:]
    
    # if the search query doesnt have results in the specified category, then check in all categories
    if (len(res)==0) & (category!='All'):
        print(' - Searching regardless the specified category')
        results = es.search(index='dimostest1', body = { "from" : 0, "size" : 1000,
            'query':{
                'bool': {
                  'must': [
                                {'match':{'title_eng':{"query" : query,
                                                       "fuzziness": "AUTO:3,5", # https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#fuzziness
                                                       #"fuzziness": 2,
                                                       #"fuzzy_transpositions":False,
                                                        "analyzer": "my_analyzer"

                                                      }}},
                                #{'match': {'Category': category}},
                           ],
#                   "filter": [ 
#                                 {"term":{"Category":category}},
#                             ]
                    
                }
            }
        },                  
                           )
        res = results['hits']['hits'][0:]
        
    results = []
    for i in range(len(res)):
        #print(res[i]['_source']['title_init']+' - ', res[i]['_source']['Price'])
        result = res[i]

        results.append({
            'Code':result['_source']['Code'],
            #'Product_name':result['_source']['title_init'],
                       #'title_eng':result['_source']['title_eng'],
                        #'Category':result['_source']['Category'],
                        #'Price':result['_source']['Price'],
                        'score':result['_score'],
                        #'img_url':result['_source']['img_url'],
                        #'Url':result['_source']['url'],
                        'Popularity':result['_source']['Popularity'],
                       })

    df_results = pd.DataFrame(results)

    if len(df_results) > 0:
        # convert list of dicts to pandas dataframe 
        #df_results = pd.DataFrame(results)

        # keep only results with relevance score higher that 4
        #df_results = df_results[df_results.score>4]

        # sort results based on relevance and Price/popularity
        df_results = df_results.sort_values(by=['score', 'Popularity'], ascending=[False, False])

        # search_flag defines whether the results are procuced by elasticsearch or based on popularity
        search_flag = 'elastic search'

    if len(df_results) == 0:
        print('- No results were returned via elasticsearch. So, return popular products.')
        df_results = df_products.sort_values(by=['freq'], ascending=[False]).head(60)
        #df_results = df_results[['Category', 'Price', 'title_eng', 'title_init']]

        # set the flag==1 - the resutls are produced based on popularity
        search_flag = 'popularity'
        
    results = df_results.to_dict('records')
    
    results2 = []
    for i in range(200):
        results2.append(results[i*5:5+(i*5)])
        
#     # if the search is very strange (e.g. ajfhr) it should give results based on popularity
#     if len(results2[0]) == 0:
#         print('Getting results based on popularity')
#         df_results = df_products.sort_values(by=['Price'], ascending=[True]).head(30)
        
#         results2 = []
#         for i in range(200):
#             results2.append(results[i*5:5+(i*5)])
    
    return results2, search_flag

