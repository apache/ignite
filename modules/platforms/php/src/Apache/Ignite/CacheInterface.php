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
interface CacheInterface
{
    /** @name PeekMode
     *  @anchor PeekMode
     *  @{
     */
    
    /**
     * Peek mode ALL
     */
    const PEEK_MODE_ALL = 0;
    
    /**
     * Peek mode NEAR
     */
    const PEEK_MODE_NEAR = 1;
    
    /**
     * Peek mode PRIMARY
     */
    const PEEK_MODE_PRIMARY = 2;
    
    /**
     * Peek mode BACKUP
     */
    const PEEK_MODE_BACKUP = 3;
    /** @} */ // end of PeekMode
    
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
     * @return CacheInterface the same instance of the cache client.
     *
     * @throws Exception::ClientException if error.
     */
    public function setKeyType($type): CacheInterface;
    
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
     * @return CacheInterface the same instance of the cache client.
     *
     * @throws Exception::ClientException if error.
     */
    public function setValueType($type): CacheInterface;
    
    /**
     * Retrieves a value associated with the specified key from the cache.
     * 
     * @param mixed $key key.
     * 
     * @return mixed value associated with the specified key, or null if it does not exist.
     * 
     * @throws Exception::ClientException if error.
     */
    public function get($key);
    
    /**
     * Retrieves entries associated with the specified keys from the cache.
     *
     * @param array $keys keys.
     *
     * @return array the retrieved entries (key-value pairs) of CacheEntry.
     *   Entries with the keys which do not exist in the cache are not included into the array.
     *
     * @throws Exception::ClientException if error.
     */
    public function getAll(array $keys): array;
    
    /**
     * Associates the specified value with the specified key in the cache.
     *
     * Overwrites the previous value if the key exists in the cache,
     * otherwise creates new entry (key-value pair).
     * 
     * @param mixed $key key
     * @param mixed $value value to be associated with the specified key.
     *
     * @throws Exception::ClientException if error.
     */
    public function put($key, $value): void;

    /**
     * Associates the specified values with the specified keys in the cache.
     *
     * Overwrites the previous value if a key exists in the cache,
     * otherwise creates new entry (key-value pair).
     *
     * @param array $entries entries (key-value pairs) of CacheEntry to be put into the cache.
     *
     * @throws Exception::ClientException if error.
     */
    public function putAll(array $entries): void;
    
    /**
     * Checks if the specified key exists in the cache.
     * 
     * @param mixed $key key to check.
     * 
     * @return bool true if the key exists, false otherwise.
     *
     * @throws Exception::ClientException if error.
     */
    public function containsKey($key): bool;
    
    /**
     * Checks if all the specified keys exist in the cache.
     * 
     * @param array $keys keys to check.
     * 
     * @return bool true if all the keys exist,
     *   false if at least one of the keys does not exist in the cache.
     * 
     * @throws Exception::ClientException if error.
     */
    public function containsKeys(array $keys): bool;
    
    /**
     * Associates the specified value with the specified key in the cache
     * and returns the previous associated value, if any.
     *
     * Overwrites the previous value if the key exists in the cache,
     * otherwise creates new entry (key-value pair).
     * 
     * @param mixed $key key.
     * @param mixed $value value to be associated with the specified key.
     * 
     * @return mixed the previous value associated with the specified key, or null if it did not exist.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getAndPut($key, $value);

    /**
     * Associates the specified value with the specified key in the cache
     * and returns the previous associated value, if the key exists in the cache.
     * Otherwise does nothing and returns null.
     * 
     * @param mixed $key key.
     * @param mixed $value value to be associated with the specified key.
     * 
     * @return mixed the previous value associated with the specified key, or null if it did not exist.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getAndReplace($key, $value);
    
    /**
     * Removes the cache entry with the specified key and returns the last associated value, if any.
     * 
     * @param mixed $key key of the entry to be removed.
     * 
     * @return mixed the last value associated with the specified key, or null if it did not exist.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getAndRemove($key);

    /**
     * Creates new entry (key-value pair) if the specified key does not exist in the cache.
     * Otherwise does nothing.
     * 
     * @param mixed $key key.
     * @param mixed $value value to be associated with the specified key.
     * 
     * @return true if the operation has been done, false otherwise.
     * 
     * @throws Exception::ClientException if error.
     */
    public function putIfAbsent($key, $value): bool;
    
    /**
     * Creates new entry (key-value pair) if the specified key does not exist in the cache.
     * Otherwise returns the current value associated with the existing key.
     * 
     * @param mixed $key key.
     * @param mixed $value value to be associated with the specified key.
     * 
     * @return mixed the current value associated with the key if it already exists in the cache,
     *   null if the new entry is created.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getAndPutIfAbsent($key, $value);
    
    /**
     * Associates the specified value with the specified key, if the key exists in the cache.
     * Otherwise does nothing.
     * 
     * @param mixed $key key.
     * @param mixed $value value to be associated with the specified key.
     * 
     * @return bool true if the operation has been done, false otherwise.
     * 
     * @throws Exception::ClientException if error.
     */
    public function replace($key, $value): bool;

    /**
     * Associates the new value with the specified key, if the key exists in the cache
     * and the current value equals to the provided one.
     * Otherwise does nothing.
     * 
     * @param mixed $key key.
     * @param mixed $value value to be compared with the current value associated with the specified key.
     * @param mixed $newValue new value to be associated with the specified key.
     * 
     * @return bool true if the operation has been done, false otherwise.
     * 
     * @throws Exception::ClientException if error.
     */
    public function replaceIfEquals($key, $value, $newValue): bool;
    
    /**
     * Removes all entries from the cache, without notifying listeners and cache writers.
     * 
     * @throws Exception::ClientException if error.
     */
    public function clear(): void;
    
    /**
     * Removes entry with the specified key from the cache, without notifying listeners and cache writers.
     * 
     * @param mixed $key key to be removed.
     *
     * @throws Exception::ClientException if error.
     */
    public function clearKey($key): void;
    
    /**
     * Removes entries with the specified keys from the cache, without notifying listeners and cache writers.
     * 
     * @param array $keys keys to be removed.
     * 
     * @throws Exception::ClientException if error.
     */
    public function clearKeys($keys): void;
    
    /**
     * Removes entry with the specified key from the cache, notifying listeners and cache writers.
     * 
     * @param mixed $key key to be removed.
     * 
     * @return bool true if the operation has been done, false otherwise.
     * 
     * @throws Exception::ClientException if error.
     */
    public function removeKey($key): bool;
    
    /**
     * Removes entry with the specified key from the cache, if the current value equals to the provided one.
     * Notifies listeners and cache writers.
     * 
     * @param mixed $key key to be removed.
     * @param mixed $value value to be compared with the current value associated with the specified key.
     * 
     * @return bool true if the operation has been done, false otherwise.
     * 
     * @throws Exception::ClientException if error.
     */
    public function removeIfEquals($key, $value): bool;
    
    /**
     * Removes entries with the specified keys from the cache, notifying listeners and cache writers.
     * 
     * @param array $keys keys to be removed.
     * 
     * @throws Exception::ClientException if error.
     */
    public function removeKeys($keys): void;
            
    /**
     * Removes all entries from the cache, notifying listeners and cache writers.
     * 
     * @throws Exception::ClientException if error.
     */
    public function removeAll(): void;
    
    /**
     * Returns the number of the entries in the cache.
     * 
     * @param int ...$peekModes peek modes, values from @ref PeekMode constants.
     * 
     * @return int the number of the entries in the cache.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getSize(int ...$peekModes): int;
}
