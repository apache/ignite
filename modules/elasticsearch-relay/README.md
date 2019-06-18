# elasticsearch-relay

可以跨数据中心进行搜索，支持两个数据中心，将结果合并为1个。
Elasticsearch relay Proxy adding CAS SSO authentication,   visibility filtering and multi-server-splitting between Elasticsearch Zone 1 and Zone 2 instances.

Only proxies Parts of the ES search API.

Supports indices from Liferay,   Nuxeo,   Shindig and the Elasticsearch IMAP importer.

License: Apache License,   Version 2.0 http://www.apache.org/licenses/LICENSE-2.0

### Usage
1. Edit src/main/resources/elasticsearch-relay.properties to match your setup
2. Build a using maven build with package goal
3. A deployable war file is generated in the target-Folder 
4. Deploy in Tomcat 7 or 8
5. Or deploy to $IgniteHome/webapp use jetty as webserver.


### Ignite backend

Ignite作为后端搜索引擎实现代替elasticsearch

Ignite schema和elasticsearch的index对应。
entity table和elasticsearch的type对应。

/dupal/content_faq/_search?query="keyword"

意思是在drupal模式里面的content_faq表里搜索keyword。

/cache/content_faq/_search?q="keyword"

意思是在cacheName为cache的content_faq表里搜索keyword。

path[0]为模式或者cacheName
path[1]为TypeName


/dupal/content_faq/_all?q=view2

意思是在drupal模式里面的content_faq表里列出view2的结果，功能是为了在后端配置视图。


支持以json的方式访问ignite rest api。

<code>

create table "drupal".content_story(   
 nid  varchar,  
 uid int,   
 status int,   
 title varchar,  
 body varchar,  
 PRIMARY KEY (nid)  
);  

create table "drupal_hr".content_resume(  
 nid  integer primary key,    
 uid integer,    
 status integer,    
 field_resume_status integer,  
 field_resume_id integer,  
 title string index using fulltext with(analyzer='dic_ansj'),  
 body string index using fulltext with(analyzer='dic_ansj'),  
 field_resume_company_now string index using fulltext with(analyzer='dic_ansj'),  
 field_resume_keywords string index using fulltext with(analyzer='dic_ansj'),  
 field_talent_key string index using fulltext with(analyzer='dic_ansj')  
)  
 
</code>
