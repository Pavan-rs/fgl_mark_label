print("start program to fetch mails with keywords for labeling")
print("importing libraries")
import os
import sys
sys.path.append('..')
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search,Q
from credentials import usr_pwd


#fgl mails label = "1658207982832FGL-Emails"
#fgl VPA label   = "1633494438604FGL VPA"

uname,pwd = usr_pwd()
hosts = ["10.1.8.6:9200"]
es = Elasticsearch(hosts,timeout = 6000, use_ssl = True, verify_certs = False, ssl_show_warn = False, http_auth=(uname,pwd))
indexes = ["cantire-2021-01","cantire-2021-02","cantire-2021-03","cantire-2021-04","cantire-2021-05","cantire-2021-06",
"cantire-2021-07","cantire-2021-08","cantire-2021-09","cantire-2021-10","cantire-2021-11","cantire-2021-12","cantire-2022-01",
"cantire-2022-02","cantire-2022-03","cantire-2022-04","cantire-2022-05","cantire-2022-06","cantire-2022-07","cantire-2022-08",
"cantire-2022-09","cantire-2022-10","cantire-2022-11"]
file__ =  open('{}_{}_{}.json'.format("FGL & FGL_VPA", 'mails', datetime.now().strftime("%d_%m_%Y_%H_%M")), 'w')

md5s = set()
for index_ in indexes:
    print("starting index",index_)
    query = {
        "bool":{
            "must":[{
                "bool":{
                    "should":[
                        {"match_phrase_prefix":{"metaData.from":"fglsports.com"}},
                        {"match_phrase_prefix":{"metaData.to":"fglsports.com"}},
                        {"match_phrase_prefix":{"metaData.cc":"fglsports.com"}}
                    ],"minimum_should_match":1
                }
            },
            {"match_phrase_prefix":{"fileType":"email"}}],
            "must_not":[
                {"match_phrase_prefix":{"labels":"1633494438604FGL VPA"}},
                {"match_phrase_prefix":{"labels":"1658207982832FGL-Emails"}}
            ]
        }
    }

    s = Search(using=es,index=index_)
    s.query = query
    print("count of mails in this index is :",s.count())
    s = s.source(["Attachment","metaData","labels","MD5","content","subject"])
    j = 0
    lis = ["VPA","Vendor Agreement","Vendor Policy Agreement","Policy Agreement"]
    vpa = False
    label_type = ""
    for x in s.scan():
        if "labels" in x:
            labels = x["labels"]
            labels = list(labels)
        else:
            labels = []
        for val in lis:
            try:
                if val in x.Attachment[0]["filename"]:
                    vpa = True
                    break
                elif val in x.content:
                    vpa = True
                    break
                elif val in x.metaData["subject"]:
                    vpa = True
                    break
            except:
                pass
        
        if vpa == True:
            label_type = "FGL-VPA"
            labels = list(labels + ["1633494438604FGL VPA"])
            try:
                file__.write('{ "update": { "_index": "'+ x.meta["index"] + '", "_type": "_doc", "_id": "'+ x.meta["id"] +'"} }\n')
                file__.write('{"doc": { "labels": ' + str(labels).replace("'", '"') + ', ' + label_type + ': "true"} }\n')
                md5s.add(x['MD5'])
            except Exception as someError:
                print("Exception ", x.meta.index, x.meta.id)
                print(someError)
        else:
            label_type = "FGL"
            if "1658207982832FGL-Emails" not in labels: labels.append("1658207982832FGL-Emails")
            try:
                file__.write('{ "update": { "_index": "'+ x.meta["index"] + '", "_type": "_doc", "_id": "'+ x.meta["id"] +'"} }\n')
                file__.write('{"doc": { "labels": ' + str(labels).replace("'", '"') + ', ' + label_type + ': "true"} }\n')
            except Exception as someError:
                print("Exception ", x.meta.index, x.meta.id)
                print(someError)

file__.close()
        
