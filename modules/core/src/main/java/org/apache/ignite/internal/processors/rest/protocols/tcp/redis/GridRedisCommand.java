/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis;

/**
 * Supported Redis-specific commands.
 * See <a href="http://redis.io/commands">Redis commands</a> for details.
 * <p>
 * Cache operations are handled via REST.
 */
public enum GridRedisCommand {
    // Connections.
    /** Ping. */
    PING("PING"),
    /** Connection close. */
    QUIT("QUIT"),
    /** Echo. */
    ECHO("ECHO"),
    /** Select **/
    SELECT("SELECT"),    
    /** always return ok */
    AUTH("AUTH"),

    // String commands.
    /** GET. */
    GET("GET"),
    /** MGET. */
    MGET("MGET"),    
    /** SET. */
    SET("SET"),
    /** MSET. */
    MSET("MSET"),
    /** INCR. */
    INCR("INCR"),
    /** DECR. */
    DECR("DECR"),
    /** INCRBY. */
    INCRBY("INCRBY"),
    /** DECRBY. */
    DECRBY("DECRBY"),
    /** APPEND. */
    APPEND("APPEND"),
    /** STRLEN. */
    STRLEN("STRLEN"),
    /** GETSET. */
    GETSET("GETSET"),
    /** SETRANGE. */
    SETRANGE("SETRANGE"),
    /** GETRANGE. */
    GETRANGE("GETRANGE"),

    // Key commands.
    /** KEYS. */
    KEYS("KEYS"),
    /** DEL. */
    DEL("DEL"),
    /** EXISTS. */
    EXISTS("EXISTS"),
    /** EXPIRE. */
    EXPIRE("EXPIRE"),
    /** PEXPIRE. */
    PEXPIRE("PEXPIRE"),
    
    // add@byron
    
    SCAN,HSCAN,SSCAN,ZSCAN,
    
    /** SET and EXPIRE. */ 
    SETEX("SETEX"),
    
    /** SET no override. */ 
    SETNX("SETNX"),
    
    // Hashes commands
    /**
    	 * @method bool|int      hSet(string $key, string $field, string $value)
    	 * @method bool          hSetNx(string $key, string $field, string $value)
    	 * @method bool|string   hGet(string $key, string $field)
    	 * @method bool|int      hLen(string $key)
    	 * @method bool          hDel(string $key, string $field)
    	 * @method array         hKeys(string $key, string $field)
    	 * @method array         hVals(string $key, string $field)
    	 * @method array         hGetAll(string $key)
    	 * @method bool          hExists(string $key, string $field)
    	 * @method int           hIncrBy(string $key, string $field, int $value)
    	 * @method bool          hMSet(string $key, array $keysValues)
    	 * @method array         hMGet(string $key, array $fields)

	*/
    
    /** hGET. */
    HGET("hGET"),
    /** hMGET. */
    HMGET("hMGET"),
    /** SET. */
    HSET("hSET"),
    
    /** hSETNX. */
    HSETNX("hSETNX"),
    
    /** MSET. */
    HMSET("hMSET"),
    
    /** hINCRBY. */
    HINCRBY("hINCRBY"),   
  
    /** hLEN. */
    HLEN("hLEN"),
    
    /** hGETALL. */
    HGETALL("hGETALL"),
    
    /** hKEYS. */
    HKEYS("hKEYS"),
    
    /** hDEL. */
    HDEL("hDEL"),
    
    /** hEXISTS. */
    HEXISTS("hEXISTS"),
    
    // List commands 
    
    LSET,
    LREM,  // LREM key count element
    LPUSH, // 将一个或多个值 value 插入到列表 key 的表头
    LPUSHX,
    RPUSH, // 将一个或多个值 value 插入到列表 key 的表尾
    RPUSHX,
    LPOP,  // 移除并返回列表 key 的头元素。
    RPOP,  // 移除并返回列表 key 的尾元素。
    BLPOP, // 是 LPOP 的阻塞版本
    BRPOP, // 是 RPOP 的阻塞版本
    LPOS,
    LRANGE, // 指定区间内的元素，区间以偏移量 START 和 END 指定
    LINDEX,
    LLEN,
    
    
    // Set commands 
    SADD,
    SREM,
    SPOP,	// 移除并返回集合中的一个随机元素
    SCARD,
    SISMEMBER,	// 判断 member 元素是否是集合 key 的成员
    SMEMBERS,	// 返回集合中的所有成员
    SDIFF,
    SINTER,
    
    // OrderedSet commands 
    ZADD,
    ZREM,
    ZPOPMAX,
    ZPOPMIN, // 删除并返回最多count个有序集合key中最低得分的成员。
    ZCARD, // 计算集合中元素的数量。
    ZRANK,
    ZREVRANK,
    ZRANGE,
    ZREVRANGE,
    ZRANGEBYSCORE, // 返回有序集 key 中， score 值介于max和min之间的所有的成员。有序集成员按 score 值递增的次序排列。
    ZREVRANGEBYSCORE, // 返回有序集 key 中， score 值介于max和min之间的所有的成员。有序集成员按 score 值递减(从大到小)的次序排列。
    
   
    
    // 发布订阅命令
 	PSUBSCRIBE,	// 订阅一个或多个符合给定模式的频道。
 	PUBSUB,	//	查看订阅与发布系统状态。
 	PUBLISH,	//	将信息发送到指定的频道。
 	PUNSUBSCRIBE,	//	退订所有给定模式的频道。
 	SUBSCRIBE,	//	订阅给定的一个或多个频道的信息。
 	UNSUBSCRIBE,	//	指退订给定的频道。
 	
    //end@
    
    // 事务 commands.
 	MULTI,
 	EXEC,
 	DISCARD,
 	
 	// Server commands.
 	CLIENT,
    /** DBSIZE. */
    DBSIZE("DBSIZE"),
    INFO,
    
    SAVE, // 所有数据的快照以 RDB 文件的形式保存到磁盘上。
    BGSAVE, // 异步保存
    /** FLUSHDB. */
    FLUSHDB("FLUSHDB 删除当前数据库的所有 key"),
    /** FLUSHALL. 删除所有数据库的所有key */
    FLUSHALL("FLUSHALL");	
	

    /** String for command. */
    private final String cmd;
	
	GridRedisCommand() {
        this.cmd = null;
    }

    /** Constructor. */
    GridRedisCommand(String cmd) {
        this.cmd = cmd;
    }
}
