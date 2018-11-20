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

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Cache\CacheInterface;
use Apache\Ignite\Internal\Connection\ClientFailoverSocket;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Utils\ArgumentChecker;
use Apache\Ignite\Internal\Utils\Logger;
use Apache\Ignite\Internal\Binary\ClientOperation;
use Apache\Ignite\Internal\Cache;

/**
 * @mainpage Public API specification
 * The thin client allows your PHP applications to work with Apache Ignite clusters via Binary %Client Protocol.
 *
 * This is the public API specification of the client.
 *
 * If you open it for the first time, start from the Client class.
 *
 * For the usage guide and other instructions see accompanied Apache Ignite PHP %Client documentation.
 */

/**
 * Class representing Ignite client.
 */
class Client
{
    private $socket;
    private $communicator;
    
    /**
     * Public Client constructor.
     */
    public function __construct()
    {
        $this->socket = new ClientFailoverSocket();
        $this->communicator = new BinaryCommunicator($this->socket);
    }
    
    /**
     * Connects the client.
     *
     * Reconnects if the client already connected.
     *
     * @param ClientConfiguration $config the client configuration.
     * 
     * @throws ClientException if error.
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
     * @return CacheInterface new instance of the class with interface representing the created cache.
     * 
     * @throws ClientException if error.
     */
    public function createCache(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->communicator->send(
            $cacheConfig ?
                ClientOperation::CACHE_CREATE_WITH_CONFIGURATION :
                ClientOperation::CACHE_CREATE_WITH_NAME,
            function (MessageBuffer $payload) use ($name, $cacheConfig)
            {
                $this->writeCacheNameOrConfig($payload, $name, $cacheConfig);
            });
        return new Cache($name, $this->communicator);
    }
    
    /**
     * Gets existing cache with the provided name
     * or creates new one with the provided name and optional configuration.
     * 
     * @param string $name cache name.
     * @param CacheConfiguration $cacheConfig cache configuration (ignored if cache
     *   with the provided name already exists).
     * 
     * @return CacheInterface new instance of the class with interface representing the existing or created cache.
     * 
     * @throws ClientException if error.
     */
    public function getOrCreateCache(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->communicator->send(
            $cacheConfig ?
                ClientOperation::CACHE_GET_OR_CREATE_WITH_CONFIGURATION :
                ClientOperation::CACHE_GET_OR_CREATE_WITH_NAME,
            function (MessageBuffer $payload) use ($name, $cacheConfig)
            {
                $this->writeCacheNameOrConfig($payload, $name, $cacheConfig);
            });
        return new Cache($name, $this->communicator);
    }
    
    /**
     * Gets instance of the class with interface representing the cache with the provided name.
     * The method does not check if the cache with the provided name exists.
     * 
     * @param string $name cache name.
     * 
     * @return CacheInterface new instance of the class with interface representing the cache.
     * 
     * @throws ClientException if error.
     */
    public function getCache(string $name): CacheInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        return new Cache($name, $this->communicator);
    }
    
    /**
     * Destroys cache with the provided name.
     *
     * @param string $name cache name.
     * 
     * @throws ClientException if error.
     */
    public function destroyCache(string $name): void
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->communicator->send(
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
     * @throws ClientException if error.
     */
    public function getCacheConfiguration(string $name): CacheConfiguration
    {
        ArgumentChecker::notEmpty($name, 'name');
        $config = null;
        $this->communicator->send(
            ClientOperation::CACHE_GET_CONFIGURATION,
            function (MessageBuffer $payload) use ($name)
            {
                $payload->writeInteger(Cache::calculateId($name));
                $payload->writeByte(0);
            },
            function (MessageBuffer $payload) use (&$config)
            {
                $config = new CacheConfiguration();
                $config->read($this->communicator, $payload);
            });
        return $config;
    }
    
    /**
     * Gets existing cache names.
     * 
     * @return array array with the existing cache names.
     *     The array is empty if no caches exist.
     * 
     * @throws ClientException if error.
     */
    public function cacheNames(): array
    {
        $names = null;
        $this->communicator->send(
            ClientOperation::CACHE_GET_NAMES,
            null,
            function (MessageBuffer $payload) use (&$names)
            {
                $names = $this->communicator->readStringArray($payload);
            });
        return $names;
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
            $cacheConfig->write($this->communicator, $buffer, $name);
        } else {
            $this->communicator->writeString($buffer, $name);
        }
    }
}
