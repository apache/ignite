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

    // String commands.
    /** GET. */
    GET("GET"),
    /** MGET. */
    MGET("MGET"),
    
    /** KEYS. */
    KEYS("KEYS"),
    
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
    /** DEL. */
    DEL("DEL"),
    /** EXISTS. */
    EXISTS("EXISTS"),
    /** EXPIRE. */
    EXPIRE("EXPIRE"),
    /** PEXPIRE. */
    PEXPIRE("PEXPIRE"),    
    // add@byron
    /** SET and EXPIRE. */ 
    SETEXPIRE("SETEX"),
    
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
    
    /** suport use */
    AUTH("AUTH"),
    //end@
    
    // Server commands.
    /** DBSIZE. */
    DBSIZE("DBSIZE"),
    /** FLUSHDB. */
    FLUSHDB("FLUSHDB"),
    /** FLUSHALL. */
    FLUSHALL("FLUSHALL");
	
	

    /** String for command. */
    private final String cmd;

    /** Constructor. */
    GridRedisCommand(String cmd) {
        this.cmd = cmd;
    }
}
