curl -X GET "localhost:9200/md_index_23/_search" -H 'Content-Type: application/json' -d'
{
    "query":{
        "bool": {
            "must": {
                "match_all": {}
            },
            "filter": {
                "geo_shape": {
                    "record.geoLocations.geoLocationBox": {
                        "shape": {
                            "type": "envelope",
                            "coordinates" : [[15.0, -20.0], [34.0, -40.0]]
                        },
                        "relation": "within"
                    }
                }
            }
        }
    }
}
'
# curl -X GET "localhost:9200/md_index_23/_search" -H 'Content-Type: application/json' -d'
# {
#     "query": {
#         "bool" : {
#             "must" : {
#                 "match_all" : {}
#             },
#             "filter" : {
#                 "geo_bounding_box" : {
#                     "record.geoLocations.geoLocationPoint" : {
#                         "top_left" : {
#                             "lat" : 40.73,
#                             "lon" : -74.1
#                         },
#                         "bottom_right" : {
#                             "lat" : 30.01,
#                             "lon" : -51.12
#                         }
#                     }
#                 }
#             }
#         }
#     }
# }
# '

