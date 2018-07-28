<?php
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

namespace Apache\Ignite;

/**
 * 
 */
interface CacheClientInterface
{
    /**
     * Specifies a type of the cache key.
     *
     * The cache client assumes that keys in all further operations with the cache
     * will have the specified type.
     * Eg. the cache client will convert keys provided as input parameters of the methods
     * to the specified object type before sending them to a server.
     *
     * After the cache client creation a type of the cache key is not specified (null).
     *
     * If the type is not specified then during operations the cache client
     * will do automatic mapping between some of the PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     *
     * @param int|ObjectType|null $type type of the keys in the cache:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (means the type is not specified).
     *
     * @return CacheClientInterface the same instance of the cache client.
     *
     * @throws Exception::IgniteClientException if error.
     */
    public function setKeyType($type): CacheClientInterface;
    
    /**
     * Specifies a type of the cache value.
     *
     * The cache client assumes that values in all further operations with the cache
     * will have the specified type.
     * Eg. the cache client will convert values provided as input parameters of the methods
     * to the specified object type before sending them to a server.
     *
     * After the cache client creation a type of the cache value is not specified (null).
     *
     * If the type is not specified then during operations the cache client
     * will do automatic mapping between some of the PHP types and object types -
     * according to the mapping table defined in the description of the ObjectType class.
     *
     * @param int|ObjectType|null $type type of the values in the cache:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (means the type is not specified).
     *
     * @return CacheClientInterface the same instance of the cache client.
     *
     * @throws Exception::IgniteClientException if error.
     */
    public function setValueType($type): CacheClientInterface;
    
    /**
     * Retrieves a value associated with the specified key from the cache.
     * 
     * @param mixed $key key.
     * 
     * @return mixed value associated with the specified key, or null if it does not exist.
     * 
     * @throws Exception::IgniteClientException if error.
     */
    public function get($key);
    
    /**
     * Associates the specified value with the specified key in the cache.
     *
     * Overwrites the previous value if the key exists in the cache,
     * otherwise creates new entry (key-value pair).
     * 
     * @param mixed $key key
     * @param mixed $value value to be associated with the specified key.
     *
     * @throws Exception::IgniteClientException if error.
     */
    public function put($key, $value);
}

