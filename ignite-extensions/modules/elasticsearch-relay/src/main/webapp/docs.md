### elasticsearch-relay

可以跨数据中心进行搜索，支持两个数据中心，将结果合并为1个。
Elasticsearch relay Proxy adding CAS SSO authentication,   visibility filtering and multi-server-splitting between Elasticsearch Zone 1 and Zone 2 instances.

Only proxies Parts of the ES search API.

Supports indices from Liferay,   Nuxeo,   Shindig and the Elasticsearch IMAP importer.




### Ignite backend

Ignite作为后端搜索引擎实现代替elasticsearch


=========
GET /{index}
Returns information about one or more indices.

关键词查询
=========
/es-relay/{index}/_search?query="keyword"

意思是在Ignite实例里面的{index}表(或者cache)搜索keyword。

/es-relay/content.faq/_search?q="keyword"

意思是在scheam为content的faq表里搜索keyword。


因为未来cache和table是一对一关系，可以直接使用schema/tableName搜索,如：
/es-relay/public.faq/_search?q="keyword"

path[1]为cacheName或者tableName
path[2]为操作


查询视图
========
/es-relay/_views/{view_name}?q=sql

意思是列出{view_name}的结果，功能是为了在后端配置SQL视图。


如： 使用视图hosp-cluster
/es-relay/_views/hosp-cluster?key=value


创建与更新
=========

PUT /<index>/_doc/<_id>

POST /<index>/_doc/

PUT /<index>/_create/<_id>

POST /<index>/_create/<_id>

操作单个文档
============

GET /{index}/_doc/{id}
获取

DELETE /{index}/_doc/{id}
删除

HEAD /{index}/_doc/{id}
是否存在

GET /{index}/_source/{id}
Returns the source of a document.

操作索引
==============
PUT /{index}
Creates an index with optional settings and mappings.

GET /{index}
Returns information about one or more indices.

DELETE /{index}
Deletes an index.

聚合命令
=======

/{index}/_count


CMD
=========
支持以json的方式访问ignite rest api。
如：
/_cmd/put?cacheName=test&key=k1
可以使用 JSON {key=k1,cacheName=test} post给服务端

