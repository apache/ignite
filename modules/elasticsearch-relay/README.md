@see webapp/docs.md

# elasticsearch-relay

可以跨数据中心进行搜索，支持两个数据中心，将结果合并为1个。
Elasticsearch relay Proxy adding CAS SSO authentication,   visibility filtering and multi-server-splitting between Elasticsearch Zone 1 and Zone 2 instances.

Only proxies Parts of the ES search API.

Supports indices from Liferay,   Nuxeo,   and the Elasticsearch IMAP importer.

License: Apache License,   Version 2.0 http://www.apache.org/licenses/LICENSE-2.0

### Usage
1. Edit src/main/resources/elasticsearch-relay.properties to match your setup
2. Build a using maven build with package goal
3. A deployable war file is generated in the target-Folder 
4. Deploy in Tomcat 7 or 8
5. Or deploy to $IgniteHome/webapps use jetty as webserver.


### Ignite backend

Ignite作为后端搜索引擎实现代替elasticsearch

Ignite cache和elasticsearch的index对应。
entity table和elasticsearch的type对应。
cache和table一对一的情况下，不用传入type参数

