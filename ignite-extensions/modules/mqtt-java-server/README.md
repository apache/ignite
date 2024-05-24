# Stuart

#### 介绍
Stuart 是一个基于 [Eclipse Vert.x](https://vertx.io) 和 [Apache Ignite](https://ignite.apache.org) 实现的 MQTT 协议服务器，具备以下特性：
1. 支持 MQTT 3.1 和 MQTT 3.1.1
2. 支持 QoS0，QoS1 和 QoS2
3. 支持订阅包含通配符(+/#)的主题
4. 支持 transient session 和 persistent session
5. 支持 persistent session 的消息持久化
6. 支持 retained 和 will 消息
7. 支持服务器集群
8. 支持基于用户名/密码的连接认证管理
9. 支持基于用户名、IP地址和主题的访问控制列表
10. 支持 Web 管理界面
11. 支持 MQTT over WebSocket

#### 安装教程
1. 运行环境<br>
   JDK：Oracle JDK 8 或 OpenJDK 8<br>
   操作系统：Linux、Mac OS X(10.6以上)、Windows(XP以上)、Windows Server(2008以上)<br>
2. 下载 release 版本，使用如下命令直接运行：`java -jar stuart-0.1.0-fat.jar`<br>
   运行后会在 stuart-0.1.0-fat.jar 文件的同级目录下创建 storage 和 log 两个目录，在未使用 --cfg 指定配置文件时，Stuart 将使用 jar 包中自带的 cfg.properties 进行启动<br>
   如果想查看启动命令的具体参数，可以使用如下命令：`java -jar stuart-0.1.0-fat.jar -h`<br>
3. 通过 maven 编译，使用 `git clone https://gitee.com/x87/stuart.git` 或 `git clone https://github.com/xyangwang/stuart.git` 命令，或者在 Stuart 项目页面直接下载源码，使用自己熟悉的 IDE 导入工程进行二次开发（需安装 lombok 插件），并使用 `mvn install` 命令编译打包。由于新增的 MQTT over WebSocket 功能修改了依赖包 `vertx-mqtt` 中的代码，如果需要自己编译，请同时到 `https://gitee.com/x87/vertx-mqtt.git` 或 `https://github.com/xyangwang/vertx-mqtt.git` 下载修改的依赖包，并 checkout `3.6` 分支代码
4. 成功启动后，就可以使用 `mosquitto_sub` 和 `mosquitto_pub` 命令或者其他 Client Library 进行开发测试

#### 系统架构
1. 系统存储

<img src="https://images.gitee.com/uploads/images/2019/0227/151339_7fcb9c6f_581533.png" />
<br>
<img src="https://images.gitee.com/uploads/images/2019/0227/151424_0ef59029_581533.png" />

系统使用 [Apache Ignite](https://ignite.apache.org) 原生持久化功能对信息进行存储<br>
存储包括：节点信息、监听器信息、连接信息、会话信息、路由信息、Will 消息、Retain 消息、管理员信息、连接用户信息和访问控制信息。<br>
节点信息、监听器信息和连接信息使用纯内存模式存储，其他信息采用持久化模式存储。<br>

路由信息包括两部分：<br>
A. topic 到 node 与 client 的映射关系；<br>
B. wildcard topic 的前缀树结构。<br>

针对持久化和非持久化两种 Session，分别采用两套不同的存储结构：<br>
A. 对于非持久化 Session，基于这类 Session 的消息不需要持久化，所以使用 On-Heap 内存对消息进行存储；<br>
B. 对于持久化 Session， 则使用 [Apache Ignite](https://ignite.apache.org) 的 IgniteCache 和 IgniteQueue 进行存储。<br>

2. 节点架构

<img src="https://images.gitee.com/uploads/images/2019/0306/094542_5482931c_581533.png" />

系统主要分成五大服务模块：<br>
A. Cache Service：为其他模块提供持久化服务；<br>
B. Auth Service：提供连接用户的认证，订阅/发布的访问控制，以及针对 local 模式下相关信息的管理配置；<br>
C. Session Service：管理连接到服务的 Session；<br>
D. Verticle Service：管理各类 Verticle 和 Service，Verticle 是各 Listener 的具体实现，为各类客户端提供连接访问；<br>
E. Metrics Service：针对 MQTT 协议的包/消息数/消息有效载荷字节数，以及 Session 运行信息的收集和统计。<br>
备注：由于 Restful API 暂时没有实现，所以上图中这部分用虚线标识。

3. 集群架构

<img src="https://images.gitee.com/uploads/images/2019/0227/151458_64ce6a04_581533.png" />

Stuart 目前可以使用两种方式组建集群：<br>
A. 静态 IP 列表；<br>
B. ZooKeeper 服务发现。

系统先通过上述方式将 [Apache Ignite](https://ignite.apache.org) 组成集群，然后再使用 [Eclipse Vert.x](https://vertx.io) 通过 [Apache Ignite](https://ignite.apache.org) 组成集群，这样就可以使用 [Eclipse Vert.x](https://vertx.io) 提供的高效的 Event Bus 在节点间进行通讯。

#### 配置文件

```
# instance configuration
# 服务器实例唯一标识
instance.id=stuart@127.0.0.1
# 服务器启动监听地址
instance.listen-address=0.0.0.0
# 服务器运行统计信息更新时间间隔
instance.metrics-period-ms=60000
# 存储路径
instance.storage.dir=./storage
# 存储写同步模式，包括：primary_sync 和 full_sync
instance.storage-write-sync-mode=primary_sync
# 预写日志模式，包括：fsync、log_only 和 background
instance.storage.wal-mode=log_only
# background 预写日志模式下缓冲区刷新到磁盘的频率
instance.storage.wal-flush-frequency-ms=2000
# 日志路径
instance.log.dir=./log
# 日志级别
instance.log.level=info

# port configuration
# MQTT 端口
mqtt.port=1883
# SSL MQTT 端口
mqtt.ssl-port=8883
# MQTT over WebSocket 端口
websocket.port=8080
# MQTT over WebSocket path
websocket.path=/mqtt
# MQTT over SSL WebSocket 端口
websocket.ssl-port=8083
# MQTT over SSL WebSocket path
websocket.ssl-path=/mqtt
# Web 端口
http.port=18083

# max connections configuration
# MQTT 最大连接数
mqtt.max-connections=102400
# SSL MQTT 最大连接数
mqtt.ssl-max-connections=1024
# MQTT over WebSocket 最大连接数
websocket.max-connections=64
# MQTT over SSL WebSocket 最大连接数
websocket.ssl-max-connections=64

# mqtt configuration
# mqtt packet max size (fixed header + variable header + payload)
# 客户端 ID 最大长度
mqtt.client.max-len=1024
# 客户端连接超时时间
mqtt.client.connect-timeout-s=30
# 客户端限制超时时间
mqtt.client.idle-timeout-s=30
# MQTT Packet 最大限制(fixed header + variable header + payload)
mqtt.packet.max-size-kb=64
# 保存 retained 消息的最大数量
mqtt.retain.max-capacity=1000000
# retained 消息 payload 最大限制
mqtt.retain.max-payload-kb=64
# retained 消息过期时间，`0`标识没有过期限制
mqtt.retain.expiry-interval-s=0
# 是否开启 SSL
mqtt.ssl-enable=false
# SSL Key 路径
mqtt.ssl-key-path=./server-key.pem
# SSL Cert 路径
mqtt.ssl-cert-path=./server-cert.pem
# 是否开启节点和会话的监控
mqtt.metrics-enable=true

# session configuration
# 发送消息时，是否对 QoS 进行升级处理，默认做降级处理，即消息与主题两者的 QoS 取较小者
session.upgrade-qos=false
# 服务器端会话等待 QoS2 消息 PUBREL 指令的最大容量
session.await-rel.max-capacity=1000
# 服务器端会话等待 QoS2 消息 PUBREL 指令的过期时间
session.await-rel.expiry-interval-s=20
# 服务器端会话队列最大容量
session.queue.max-capacity=1000
# 服务器端会话队列是否存储 QoS0 消息
session.queue.store-qos0=false
# 服务器端会话飞行窗口最大容量
session.inflight.max-capacity=32
# 服务器端会话飞行窗口中消息的过期时间
session.inflight.expiry-interval-s=20
# 服务器端会话飞行窗口中消息的重试次数
session.inflight.max-retries=3

# authentication and authorization basic configuration
# system use md5(auth.aes-key) value as AES Algorithm's Key
# 连接用户加解密秘钥
auth.aes-key=1234567890
# 是否允许匿名链接访问
auth.allow-anonymous=false
# 是否允许不匹配访问控制列表的访问
auth.acl-allow-nomatch=true
# 认证鉴权方式，包括：local、redis、mysql、postgre 和 mongo
# 只有在 local 模式下，才可以使用 Web 管理界面中的“连接用户”和“访问控制”界面
auth.mode=local

# authentication and authorization redis configuration
auth.redis.host=127.0.0.1
auth.redis.port=6379
auth.redis.pass=
auth.redis.select=0
auth.redis.user-key-prefix=stuart:auth_user:
auth.redis.passwd-field=password
auth.redis.acl.user-key-prefix=stuart:acl_user:
auth.redis.acl.ipaddr-key-prefix=stuart:acl_ipaddr:
auth.redis.acl.client-key-prefix=stuart:acl_client:
auth.redis.acl.all-key-prefix=stuart:acl_all:

# authentication and authorization mysql/postgresql server configuration
auth.rdb.host=127.0.0.1
auth.rdb.port=3306
auth.rdb.username=root
auth.rdb.password=123456
auth.rdb.database=stuart
auth.rdb.charset=UTF-8
auth.rdb.max-pool-size=10
auth.rdb.query-timeout-ms=10000

# authentication and authorization mongodb configuration
auth.mongo.host=127.0.0.1
auth.mongo.port=27017
auth.mongo.db-name=stuart
auth.mongo.username=root
auth.mongo.password=123456
auth.mongo.auth-source=stuart
auth.mongo.auth-mechanism=SCRAM-SHA-1
auth.mongo.max-pool-size=100
auth.mongo.min-pool-size=10
auth.mongo.max-idle-time-ms=0
auth.mongo.max-life-time-ms=0
auth.mongo.wait-queue-multiple=500
auth.mongo.wait-queue-timeout-ms=120000
auth.mongo.maintenance-frequency-ms=1000
auth.mongo.maintenance-initial-delay-ms=0
auth.mongo.connect-timeout-ms=10000
auth.mongo.socket-timeout-ms=0
auth.mongo.user=stuart_user
auth.mongo.user.username-field=username
auth.mongo.user.password-field=password
auth.mongo.acl=stuart_acl
auth.mongo.acl.target-field=target
auth.mongo.acl.type-field=type
auth.mongo.acl.seq-field=seq
auth.mongo.acl.topics-field=topics
auth.mongo.acl.topic-field=topic
auth.mongo.acl.authority-field=authority

# vert.x configuration
# 允许多个启动多个 MQTT Verticle
vertx.multi-instances-enable=false
# MQTT Verticle 数量
vertx.multi-instances=1
# 工作线程池大小
vertx.worker-pool-size=2
# 开启文件缓存，提高 Web 管理界面访问速度，建议在开发时设置为 false
vertx.file-caching.enabled=true
# Web 管理界面 Session 超时时间
vertx.http.session-timeout-ms=3600000

# cluster configuration
# cluster storage backups(if mode is standalone, backups = 0)
# 集群模式，包括：standalone、vmip 和 zookeeper
cluster.mode=standalone
# 集群持久化备份数量
cluster.storage-backups=2
# 集群拓扑重新平衡触发时间间隔
cluster.blt-rebalance-time-ms=300000

# cluster vmip configuration
# 静态 IP 地址列表
vmip.addresses=127.0.0.1:47500..47509

# cluster zookeeper configuration
# session timeout must be bigger than zookeeper tickTime * syncLimit
zookeeper.connect-string=127.0.0.1:2181
zookeeper.root-path=/stuart
zookeeper.join-timeout-ms=10000
zookeeper.session-timeout-ms=20000
zookeeper.reconnect.enable=true
```
备注：关于 Apache Ignite 的WAL模式可参考[这里](https://liyuj.gitee.io/doc/java/Persistence.html#_16-2-2-wal模式)

#### Roadmap
1. 针对 transient session 的 Off-Heap 优化
2. Restful API
3. Unit test
4. Benchmark
5. Remove JQuery :cry: 
6. Remember me
7. ...

#### Web 管理界面
1. 在成功启动后，可以使用 Stuart 提供的 Web 管理界面对服务器进行监控和管理
2. 管理界面访问地址 `http://127.0.0.1:18083`
3. 初始化用户名/密码：`admin/stuart`
4. 部分管理界面如下所示：
![登录界面](https://images.gitee.com/uploads/images/2019/0221/213413_dbe5c333_581533.png "屏幕快照 2019-02-21 21.23.01.png")
![控制台](https://images.gitee.com/uploads/images/2019/0221/213447_eb015c11_581533.png "屏幕快照 2019-02-21 21.26.03.png")
![连接](https://images.gitee.com/uploads/images/2019/0221/213529_0fa3ae48_581533.png "屏幕快照 2019-02-21 21.26.11.png")
![会话](https://images.gitee.com/uploads/images/2019/0221/213613_df9920d9_581533.png "屏幕快照 2019-02-21 21.26.16.png")
![主题](https://images.gitee.com/uploads/images/2019/0221/213631_2b55f7c2_581533.png "屏幕快照 2019-02-21 21.26.23.png")
![订阅](https://images.gitee.com/uploads/images/2019/0221/213651_af510501_581533.png "屏幕快照 2019-02-21 21.26.29.png")
![连接用户](https://images.gitee.com/uploads/images/2019/0221/213754_5251e8f4_581533.png "屏幕快照 2019-02-21 21.26.34.png")
![访问控制](https://images.gitee.com/uploads/images/2019/0221/213837_d658b814_581533.png "屏幕快照 2019-02-21 21.26.39.png")
![监听器](https://images.gitee.com/uploads/images/2019/0221/213903_5ca05ccb_581533.png "屏幕快照 2019-02-21 21.26.44.png")
![WebSocket](https://images.gitee.com/uploads/images/2019/0306/093713_1e86ac00_581533.png "屏幕快照 2019-03-06 09.27.47.png")
![系统管理员](https://images.gitee.com/uploads/images/2019/0221/213934_b2d1584c_581533.png "屏幕快照 2019-02-21 21.26.49.png")
5. 备注：由于 Restful API 功能暂未开发，所以这部分只是静态界面

#### License
Copyright 2019 Yang Wang
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.