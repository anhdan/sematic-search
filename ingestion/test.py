from ESClient import ESClient


es = ESClient( "../config/martin_es.json" )

print( es.info() )