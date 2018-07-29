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

use Apache\Ignite\Impl\Connection\ClientFailoverSocket;
use Apache\Ignite\Impl\Binary\MessageBuffer;
use Apache\Ignite\Impl\Utils\ArgumentChecker;
use Apache\Ignite\Impl\Utils\Logger;
use Apache\Ignite\Impl\Binary\ClientOperation;
use Apache\Ignite\Impl\Binary\BinaryWriter;
use Apache\Ignite\Impl\Cache;

/**
 * Class representing Ignite client.
 */
class Client
{
    private $socket;
    
    /**
     * Public constructor.
     * 
     * @return Client new Client instance.
     */
    public function __construct()
    {
        $this->socket = new ClientFailoverSocket();
    }
    
    /**
     * Connects the client.
     *
     * Reconnects if the client already connected.
     *
     * @param ClientConfiguration $config the client configuration.
     * 
     * @throws Exception::ClientException if error.
     */
    public function connect(ClientConfiguration $config): void
    {
        $this->socket->connect($config);
    }
    
    /**
     * Disconnects the client.
     *
     * Does nothing if the client already disconnected.
     */
    public function disconnect(): void
    {
        $this->socket->disconnect();
    }
    
    /**
     * Creates new cache with the provided name and optional configuration.
     * 
     * @param string $name cache name.
     * @param CacheConfiguration $cacheConfig optional cache configuration.
     * 
     * @return CacheInterface new cache instance for the created cache.
     * 
     * @throws Exception::ClientException if error.
     */
    public function createCache(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->socket->send(
            $cacheConfig ?
                ClientOperation::CACHE_CREATE_WITH_CONFIGURATION :
                ClientOperation::CACHE_CREATE_WITH_NAME,
            function (MessageBuffer $payload) use ($name, $cacheConfig) {
                $this->writeCacheNameOrConfig($payload, $name, $cacheConfig);
            });
        return new Cache($name, $this->socket);
    }
    
    /**
     * Gets existing cache with the provided name
     * or creates new one with the provided name and optional configuration.
     * 
     * @param string $name cache name.
     * @param CacheConfiguration $cacheConfig cache configuration (ignored if cache
     *   with the provided name already exists).
     * 
     * @return CacheInterface new cache instance for the existing or created cache.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getOrCreateCache(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->socket->send(
            $cacheConfig ?
                ClientOperation::CACHE_GET_OR_CREATE_WITH_CONFIGURATION :
                ClientOperation::CACHE_GET_OR_CREATE_WITH_NAME,
            function (MessageBuffer $payload) use ($name, $cacheConfig) {
                $this->writeCacheNameOrConfig($payload, $name, $cacheConfig);
            });
        return new Cache($name, $this->socket);
    }
    
    /**
     * Gets cache instance of cache with the provided name.
     * The method does not check if the cache with the provided name exists.
     * 
     * @param string $name cache name.
     * 
     * @return CacheInterface new cache instance.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getCache(string $name): CacheInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        return new Cache($name, $this->socket);
    }
    
    /**
     * Destroys cache with the provided name.
     *
     * @param string $name cache name.
     * 
     * @throws Exception::ClientException if error.
     */
    public function destroyCache(string $name): void
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->socket->send(
            ClientOperation::CACHE_DESTROY,
            function (MessageBuffer $payload) use ($name)
            {
                $payload->writeInteger(Cache::calculateId($name));
            });
    }
    
    /**
     * Returns configuration of cache with the provided name.
     * 
     * @param string $name cache name.
     * 
     * @return CacheConfiguration cache configuration.
     * 
     * @throws Exception::ClientException if error.
     */
    public function getCacheConfiguration(string $name): CacheConfiguration
    {
    }
    
    /**
     * Gets existing cache names.
     * 
     * @return array array with the existing cache names.
     *     The array is empty if no caches exist.
     * 
     * @throws Exception::ClientException if error.
     */
    public function cacheNames(): array
    {
    }
    
    /**
     * Enables/disables the Ignite client's debug output (including errors logging).
     * Disabled by default.
     * 
     * @param bool $value true to enable, false to disable.
     */
    public function setDebug(bool $value): void
    {
        Logger::setDebug($value);
    }
    
    private function writeCacheNameOrConfig(
            MessageBuffer $buffer,
            string $name,
            CacheConfiguration $cacheConfig = null): void
    {
        if ($cacheConfig) {
            $cacheConfig->write($buffer, $name);
        } else {
            BinaryWriter::writeString($buffer, $name);
        }
    }
}
