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

namespace Apache\Ignite\Impl;

use Apache\Ignite\CacheClientInterface;
use Apache\Ignite\CacheConfiguration;
use Apache\Ignite\ObjectType\ObjectType;
use Apache\Ignite\Impl\Binary\ClientOperation;
use Apache\Ignite\Impl\Binary\MessageBuffer;
use Apache\Ignite\Impl\Connection\ClientFailoverSocket;
use Apache\Ignite\Impl\Utils\ArgumentChecker;
use Apache\Ignite\Impl\Binary\BinaryUtils;
use Apache\Ignite\Impl\Binary\BinaryWriter;
use Apache\Ignite\Impl\Binary\BinaryReader;

class CacheClient implements CacheClientInterface
{
    private $name;
    private $cacheId;
    private $config;
    private $keyType;
    private $valueType;
    private $socket;
    
    public function __construct(string $name, ?CacheConfiguration $config, ClientFailoverSocket $socket)
    {
        $this->name = $name;
        $this->cacheId = CacheClient::calculateId($this->name);
        $this->config = $config;
        $this->socket = $socket;
        $this->keyType = null;
        $this->valueType = null;
    }
    
    public static function calculateId($name)
    {
        return BinaryUtils::hashCode($name);
    }
    
    public function setKeyType($type): CacheClientInterface
    {
        $this->keyType = $type;
        return $this;
    }

    public function setValueType($type): CacheClientInterface
    {
        $this->valueType = $type;
        return $this;
    }
    
    public function get($key)
    {
        return $this->writeKeyReadValueOp(ClientOperation::CACHE_GET, $key);
    }
    
    public function put($key, $value): void
    {
        $this->writeKeyValueOp(ClientOperation::CACHE_PUT, $key, $value);        
    }
    
    private function writeKeyValueOp(int $operation, $key, $value, ?callable $payloadReader = null): void
    {
        ArgumentChecker::notNull($key, 'key');
        ArgumentChecker::notNull($value, 'value');
        $this->socket->send(
            $operation,
            function (MessageBuffer $payload) use ($key, $value) {
                $this->writeCacheInfo($payload);
                $this->writeKeyValue($payload, $key, $value);
            },
            $payloadReader);
    }
            
    private function writeKeyReadValueOp(int $operation, $key)
    {
        $value = null;
        $this->writeKeyOp(
            $operation, $key,
            function (MessageBuffer $payload) use (&$value) {
                $value = BinaryReader::readObject($payload, $this->valueType);
            });
        return $value;
    }
    
    private function writeKeyOp(int $operation, $key, callable $payloadReader = null): void
    {
        ArgumentChecker::notNull($key, 'key');
        $this->socket->send(
            $operation,
            function (MessageBuffer $payload) use ($key) {
                $this->writeCacheInfo($payload);
                BinaryWriter::writeObject($payload, $key, $this->keyType);
            },
            $payloadReader);
    }
    
    private function writeCacheInfo(MessageBuffer $payload): void
    {
        $payload->writeInteger($this->cacheId);
        $payload->writeByte(0);
    }
    
    private function writeKeyValue(MessageBuffer $payload, $key, $value): void
    {
        BinaryWriter::writeObject($payload, $key, $this->keyType);
        BinaryWriter::writeObject($payload, $value, $this->valueType);
    }
}
