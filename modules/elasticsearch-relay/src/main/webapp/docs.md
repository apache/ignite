### elasticsearch-relay

可以跨数据中心进行搜索，支持两个数据中心，将结果合并为1个。
Elasticsearch relay Proxy adding CAS SSO authentication,   visibility filtering and multi-server-splitting between Elasticsearch Zone 1 and Zone 2 instances.

Only proxies Parts of the ES search API.

Supports indices from Liferay,   Nuxeo,   Shindig and the Elasticsearch IMAP importer.




### Ignite backend

Ignite作为后端搜索引擎实现代替elasticsearch

Ignite schema和elasticsearch的index对应。
entity table和elasticsearch的type对应。

/drupal/content_faq/_search?query="keyword"

意思是在drupal模式里面的content_faq表里搜索keyword。

/cache/content_faq/_search?q="keyword"

意思是在cacheName为cache的content_faq表里搜索keyword。

path[0]为模式或者cacheName
path[1]为TypeName


/drupal/content_faq/_all?q=view2

意思是在drupal模式里面的content_faq表里列出view2的结果，功能是为了在后端配置视图。


支持以json的方式访问ignite rest api。
如：
/_cmd/put?cacheName=test&key=k1

如： 使用视图hosp-cluster
/HosQualityReportCache/HosQualityReport/_all?q=hosp-cluster

