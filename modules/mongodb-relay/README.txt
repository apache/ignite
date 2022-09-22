Ignite MongoDB Backend
======================================
Ignite imple mongo proprote act as mongdb server

Ignite Cache 和  Mongo Collection 是对应的。
_class 为type
_id    为key

使用lucene作为index，一个collection对应一个cache

默认的DataBase为default，不能创建新的数据库，只能通过配置文件增加数据库

必须配置admin数据库
当ignite的instance name 为admin时，不需要配置MongoPluginConfiguration，使用默认设置即可启动mongo协议。



Why 
===============
Ignite的全文检索能力比较差，使用mongodb协议支持全文检索


Key type support
=================
LongID
StringID
