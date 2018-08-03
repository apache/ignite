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

use Apache\Ignite\CacheInterface;
use Apache\Ignite\CacheEntry;
use Apache\Ignite\Query\Query;
use Apache\Ignite\Query\CursorInterface;
use Apache\Ignite\Impl\Binary\ClientOperation;
use Apache\Ignite\Impl\Binary\MessageBuffer;
use Apache\Ignite\Impl\Connection\ClientFailoverSocket;
use Apache\Ignite\Impl\Utils\ArgumentChecker;
use Apache\Ignite\Impl\Binary\BinaryUtils;
use Apache\Ignite\Impl\Binary\BinaryWriter;
use Apache\Ignite\Impl\Binary\BinaryReader;

class Cache implements CacheInterface
{
    private $name;
    private $id;
    private $keyType;
    private $valueType;
    private $socket;
    
    public function __construct(string $name, ClientFailoverSocket $socket)
    {
        $this->name = $name;
        $this->id = Cache::calculateId($this->name);
        $this->socket = $socket;
        $this->keyType = null;
        $this->valueType = null;
    }
    
    public static function calculateId(string $name)
    {
        return BinaryUtils::hashCode($name);
    }
    
    public function setKeyType($type): CacheInterface
    {
        BinaryUtils::checkObjectType($type, 'type');
        $this->keyType = $type;
        return $this;
    }

    public function setValueType($type): CacheInterface
    {
        BinaryUtils::checkObjectType($type, 'type');
        $this->valueType = $type;
        return $this;
    }
    
    public function get($key)
    {
        return $this->writeKeyReadValueOp(ClientOperation::CACHE_GET, $key);
    }
    
    public function getAll(array $keys): array
    {
        ArgumentChecker::notEmpty($keys, 'keys');
        $result = [];
        $this->socket->send(
            ClientOperation::CACHE_GET_ALL,
            function (MessageBuffer $payload) use ($keys) {
                $this->writeCacheInfo($payload);
                $this->writeKeys($payload, $keys);
            },
            function (MessageBuffer $payload) use (&$result) {
                $resultCount = $payload->readInteger();
                for ($i = 0; $i < $resultCount; $i++) {
                    array_push($result, new CacheEntry(
                        BinaryReader::readObject($payload, $this->keyType),
                        BinaryReader::readObject($payload, $this->valueType)));
                }
            });
        return $result;
    }
    
    public function put($key, $value): void
    {
        $this->writeKeyValueOp(ClientOperation::CACHE_PUT, $key, $value);        
    }

    public function putAll(array $entries): void
    {
        ArgumentChecker::notEmpty($entries, 'entries');
        ArgumentChecker::hasType($entries, 'entries', true, CacheEntry::class);
        $this->socket->send(
            ClientOperation::CACHE_PUT_ALL,
            function (MessageBuffer $payload) use ($entries) {
                $this->writeCacheInfo($payload);
                $payload->writeInteger(count($entries));
                foreach ($entries as $entry) {
                    $this->writeKeyValue($payload, $entry->getKey(), $entry->getValue());
                }
            });
    }
    
    public function containsKey($key): bool
    {
        return $this->writeKeyReadBooleanOp(ClientOperation::CACHE_CONTAINS_KEY, $key);
    }
    
    public function containsKeys(array $keys): bool
    {
        return $this->writeKeysReadBooleanOp(ClientOperation::CACHE_CONTAINS_KEYS, $keys);
    }
    
    public function getAndPut($key, $value)
    {
        return $this->writeKeyValueReadValueOp(ClientOperation::CACHE_GET_AND_PUT, $key, $value);
    }

    public function getAndReplace($key, $value)
    {
        return $this->writeKeyValueReadValueOp(ClientOperation::CACHE_GET_AND_REPLACE, $key, $value);
    }
    
    public function getAndRemove($key)
    {
        return $this->writeKeyReadValueOp(ClientOperation::CACHE_GET_AND_REMOVE, $key);
    }

    public function putIfAbsent($key, $value): bool
    {
        return $this->writeKeyValueReadBooleanOp(ClientOperation::CACHE_PUT_IF_ABSENT, $key, $value);
    }
    
    public function getAndPutIfAbsent($key, $value)
    {
        return $this->writeKeyValueReadValueOp(ClientOperation::CACHE_GET_AND_PUT_IF_ABSENT, $key, $value);
    }
    
    public function replace($key, $value): bool
    {
        return $this->writeKeyValueReadBooleanOp(ClientOperation::CACHE_REPLACE, $key, $value);
    }

    public function replaceIfEquals($key, $value, $newValue): bool
    {
        ArgumentChecker::notNull($key, 'key');
        ArgumentChecker::notNull($value, 'value');
        ArgumentChecker::notNull($newValue, 'newValue');
        $result = false;
        $this->socket->send(
            ClientOperation::CACHE_REPLACE_IF_EQUALS,
            function (MessageBuffer $payload) use ($key, $value, $newValue) {
                $this->writeCacheInfo($payload);
                $this->writeKeyValue($payload, $key, $value);
                BinaryWriter::writeObject($payload, $newValue, $this->valueType);
            },
            function (MessageBuffer $payload) use (&$result) {
                $result = $payload->readBoolean();
            });
        return $result;
    }
    
    public function clear(): void
    {
        $this->socket->send(
            ClientOperation::CACHE_CLEAR,
            function (MessageBuffer $payload) {
                $this->writeCacheInfo($payload);
            });
    }
    
    public function clearKey($key): void
    {
        $this->writeKeyOp(ClientOperation::CACHE_CLEAR_KEY, $key);
    }
    
    public function clearKeys($keys): void
    {
        $this->writeKeysOp(ClientOperation::CACHE_CLEAR_KEYS, $keys);
    }
    
    public function removeKey($key): bool
    {
        return $this->writeKeyReadBooleanOp(ClientOperation::CACHE_REMOVE_KEY, $key);
    }
    
    public function removeIfEquals($key, $value): bool
    {
        return $this->writeKeyValueReadBooleanOp(ClientOperation::CACHE_REMOVE_IF_EQUALS, $key, $value);
    }
    
    public function removeKeys($keys): void
    {
        $this->writeKeysOp(ClientOperation::CACHE_REMOVE_KEYS, $keys);
    }
            
    public function removeAll(): void
    {
        $this->socket->send(
            ClientOperation::CACHE_REMOVE_ALL,
            function (MessageBuffer $payload) {
                $this->writeCacheInfo($payload);
            });
    }
    
    public function getSize(...$peekModes): int
    {
        ArgumentChecker::hasValueFrom($peekModes, 'peekModes', true, [
            CacheInterface::PEEK_MODE_ALL,
            CacheInterface::PEEK_MODE_NEAR,
            CacheInterface::PEEK_MODE_PRIMARY,
            CacheInterface::PEEK_MODE_BACKUP
        ]);
        $result = 0;
        $this->socket->send(
            ClientOperation::CACHE_GET_SIZE,
            function (MessageBuffer $payload) use ($peekModes) {
                $this->writeCacheInfo($payload);
                $payload->writeInteger(count($peekModes));
                foreach ($peekModes as $mode) {
                    $payload->writeByte($mode);
                }
            },
            function (MessageBuffer $payload) use (&$result) {
                $result = $payload->readLong();
            });
        return $result;
    }
    
    public function query(Query $query): CursorInterface
    {
        $value = null;
        $this->socket->send(
            $query->getOperation(),
            function (MessageBuffer $payload) use ($query) {
                $this->writeCacheInfo($payload);
                $query->write($payload);
            },
            function (MessageBuffer $payload) use ($query, &$value) {
                $value = $query->getCursor($this->socket, $payload, $this->keyType, $this->valueType);
            });
        return $value;
    }
    
    private function writeCacheInfo(MessageBuffer $payload): void
    {
        $payload->writeInteger($this->id);
        $payload->writeByte(0);
    }
    
    private function writeKeyValueOp(int $operation, $key, $value, callable $payloadReader = null): void
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
    
    private function writeKeyValueReadValueOp(int $operation, $key, $value)
    {
        $result = null;
        $this->writeKeyValueOp(
            $operation, $key, $value,
            function (MessageBuffer $payload) use (&$result) {
                $result = BinaryReader::readObject($payload, $this->valueType);
            });
        return $result;
    }
    
    private function writeKeyValueReadBooleanOp(int $operation, $key, $value): bool
    {
        $result = false;
        $this->writeKeyValueOp(
            $operation, $key, $value,
            function (MessageBuffer $payload) use (&$result) {
                $result = $payload->readBoolean();
            });
        return $result;
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
    
    private function writeKeyReadBooleanOp(int $operation, $key): bool
    {
        $result = false;
        $this->writeKeyOp(
            $operation,
            $key,
            function (MessageBuffer $payload) use (&$result) {
                $result = $payload->readBoolean();
            });
        return $result;
        
    }
    
    private function writeKeys(MessageBuffer $payload, array $keys): void
    {
        $payload->writeInteger(count($keys));
        foreach ($keys as $key) {
            BinaryWriter::writeObject($payload, $key, $this->keyType);
        }
    }

    private function writeKeysReadBooleanOp(int $operation, array $keys): bool
    {
        $result = false;
        $this->writeKeysOp(
            $operation,
            $keys,
            function (MessageBuffer $payload) use (&$result) {
                $result = $payload->readBoolean();
            });
        return $result;
    }
    
    private function writeKeysOp(int $operation, array $keys, callable $payloadReader = null): void
    {
        ArgumentChecker::notEmpty($keys, 'keys');
        $this->socket->send(
            $operation,
            function (MessageBuffer $payload) use ($keys) {
                $this->writeCacheInfo($payload);
                $this->writeKeys($payload, $keys);
            },
            $payloadReader);
    }
    
    private function writeKeyValue(MessageBuffer $payload, $key, $value): void
    {
        BinaryWriter::writeObject($payload, $key, $this->keyType);
        BinaryWriter::writeObject($payload, $value, $this->valueType);
    }
}
