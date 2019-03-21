# ElasticSpark
Insert JSON data and query to/from Elasticsearch using Java and Spark.

<br />
<br />

## Details
### Elasticsearch
* Index: testen
* Type: _doc
* _id: MD5 of link

### Program Arguments
#### Write
args[0]
<pre>
{
  "ip": "Elasticsearch IP address",
  "port": "Elasticsearch port",
  "index": "Elasticsearch index",
  "type": "Elasticsearch type"
}
</pre>

args[1]: path to JSON file

#### Read
args[0]
<pre>
{
  "ip": "Elasticsearch IP address",
  "port": "Elasticsearch port",
  "index": "Elasticsearch index",
  "type": "Elasticsearch type"
}
</pre>

args[1]: query

<br />
<br />

## How to compile
<pre>mvn install</pre>

<br />
<br />

## Running the program
### Write
<pre>
spark-submit --class com.malik.main.WriteMain --master local[2] malik/engine/ElasticSpark-1.0-SNAPSHOT-jar-with-dependencies.jar '{"ip":"192.168.20.11,192.168.20.12","port":"9200","index":"testen","type":"_doc"}' '/home/malik/data/News_Category_Dataset_v2.json'
</pre>

### Read
<pre>
spark-submit --class com.malik.main.ReadMain --master local[2] malik/engine/ElasticSpark-1.0-SNAPSHOT-jar-with-dependencies.jar '{"ip":"192.168.20.11,192.168.20.12","port":"9200","index":"testen","type":"_doc"}' '{"query":{"match":{"category":"sports"}}}'
</pre>
