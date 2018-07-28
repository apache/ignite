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
use Apache\Ignite\Impl\CacheClient;

/**
 * Class representing Ignite client.
 */
class IgniteClient
{
    private $socket;
    
    /**
     * Public constructor.
     * 
     * @return IgniteClient new IgniteClient instance.
     */
    public function __construct()
    {
        $this->socket = new ClientFailoverSocket();
    }
    
    /**
     * Connects the client.
     *
     * @param IgniteClientConfiguration $config the client configuration.
     * 
     * @throws Exception::IllegalStateException if the client is not in DISCONNECTED state.
     * @throws Exception::IgniteClientException if other error.
     */
    public function connect(IgniteClientConfiguration $config): void
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
     * @return CacheClientInterface new cache client instance for the created cache.
     * 
     * @throws IllegalStateException if the client is not in CONNECTED state.
     * @throws OperationException if cache with the provided name already exists.
     * @throws IgniteClientException if other error.
     */
    public function createCache(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheClientInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->socket->send(
            $cacheConfig ?
                ClientOperation::CACHE_CREATE_WITH_CONFIGURATION :
                ClientOperation::CACHE_CREATE_WITH_NAME,
            function (MessageBuffer $payload) use ($name, $cacheConfig) {
                $this->writeCacheNameOrConfig($payload, $name, $cacheConfig);
            });
        return $this->getCacheClient($name, $cacheConfig);
    }
    
    /**
     * 
     * @param string $name
     * @param CacheConfiguration $cacheConfig
     * 
     * @return CacheClientInterface
     */
    public function getOrCreateCache(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheClientInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->socket->send(
            $cacheConfig ?
                ClientOperation::CACHE_GET_OR_CREATE_WITH_CONFIGURATION :
                ClientOperation::CACHE_GET_OR_CREATE_WITH_NAME,
            function (MessageBuffer $payload) use ($name, $cacheConfig) {
                $this->writeCacheNameOrConfig($payload, $name, $cacheConfig);
            });
        return $this->getCacheClient($name, $cacheConfig);
    }
    
    /**
     * 
     * @param string $name
     * 
     * @return CacheClientInterface
     */
    public function getCache(string $name): CacheClientInterface
    {
        ArgumentChecker::notEmpty($name, 'name');
        return $this->getCacheClient($name);
    }
    
    /**
     * 
     * @param string $name
     */
    public function destroyCache(string $name): void
    {
        ArgumentChecker::notEmpty($name, 'name');
        $this->socket->send(
            ClientOperation::CACHE_DESTROY,
            function (MessageBuffer $payload) use ($name)
            {
                $payload->writeInteger(CacheClient::calculateId($name));
            });
    }
    
    /**
     * 
     * @param bool $value
     */
    public function setDebug(bool $value): void
    {
        Logger::setDebug($value);
    }
    
    private function getCacheClient(
            string $name,
            CacheConfiguration $cacheConfig = null): CacheClientInterface
    {
        return new CacheClient($name, $cacheConfig, $this->socket);
    }
    
    private function writeCacheNameOrConfig(
            MessageBuffer $buffer,
            string $name,
            CacheConfiguration $cacheConfig = null): void
    {
        if ($cacheConfig) {
            $cacheConfig->write($buffer, $name);
        }
        else {
            BinaryWriter::writeString($buffer, $name);
        }
    }
}
