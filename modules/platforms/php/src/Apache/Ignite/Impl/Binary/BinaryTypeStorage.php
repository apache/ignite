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

namespace Apache\Ignite\Impl\Binary;

use Ds\Map;
use Apache\Ignite\Impl\Connection\ClientFailoverSocket;
use Apache\Ignite\Type\ComplexObjectType;

class BinaryTypeStorage
{
    private static $entity;
    
    private $socket;
    private $types;
    private $complexObjectTypes;

    private function __construct(ClientFailoverSocket $socket)
    {
        $this->socket = $socket;
        $this->types = [];
        $this->complexObjectTypes = new Map();
    }
    
    public static function getEntity(): BinaryTypeStorage
    {
        if (!BinaryTypeStorage::$entity) {
            BinaryUtils::internalError();
        }
        return BinaryTypeStorage::$entity;
    }

    public static function createEntity(ClientFailoverSocket $socket): void
    {
        BinaryTypeStorage::$entity = new BinaryTypeStorage($socket);
    }

    public function addType(BinaryType $binaryType, BinarySchema $binarySchema): void
    {
        $typeId = $binaryType->getId();
        $schemaId = $binarySchema->getId();
        $storageType = $this->getStorageType($typeId);
        if (!$storageType || !$storageType->hasSchema($schemaId)) {
            $binaryType->addSchema($binarySchema);
            if (!$storageType) {
                $this->types[$typeId] = $binaryType;
                $storageType = $binaryType;
            } else {
                $storageType->merge($binaryType, $binarySchema);
            }
            $this->putBinaryType($binaryType);
        }
    }
    
    public function getType(int $typeId, int $schemaId = null): ?BinaryType
    {
        $storageType = $this->getStorageType($typeId);
        if (!$storageType || $schemaId && !$storageType->hasSchema($schemaId)) {
            $storageType = $this->getBinaryType($typeId);
            if ($storageType) {
                $this->types[$storageType->getId()] = $storageType;
            }
        }
        return $storageType;
    }

    public function getByComplexObjectType(ComplexObjectType $complexObjectType): ?array
    {
        return $this->complexObjectTypes->get($complexObjectType, null);
    }

    public function setByComplexObjectType(ComplexObjectType $complexObjectType, BinaryType $type, BinarySchema $schema): void
    {
        if (!$this->complexObjectTypes->hasKey($complexObjectType)) {
            $this->complexObjectTypes->put($complexObjectType, [$type, $schema]);
        }
    }

    private function getBinaryType(int $typeId): ?BinaryType
    {
        $binaryType = new BinaryType(null);
        $binaryType->setId($typeId);
        $this->socket->send(
            ClientOperation::GET_BINARY_TYPE,
            function (MessageBuffer $payload) use ($typeId) {
                $payload->writeInteger($typeId);
            },
            function (MessageBuffer $payload) use (&$binaryType) {
                $exist = $payload->readBoolean();
                if ($exist) {
                    $binaryType->read($payload);
                } else {
                    $binaryType = null;
                }
            });
        return $binaryType;
    }
    
    private function putBinaryType(BinaryType $binaryType): void
    {
        $this->socket->send(
            ClientOperation::PUT_BINARY_TYPE,
            function (MessageBuffer $payload) use ($binaryType) {
                $binaryType->write($payload);
            });
    }

    private function getStorageType(int $typeId): ?BinaryType
    {
        return array_key_exists($typeId, $this->types) ? $this->types[$typeId] : null;
    }
}
