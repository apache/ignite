<?php
/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache\Ignite\Internal\Binary;

use Ds\Map;
use Apache\Ignite\Type\ComplexObjectType;

class BinaryTypeStorage
{
    private $communicator;
    private $types;
    private static $complexObjectTypes = null;

    public function __construct(BinaryCommunicator $communicator)
    {
        $this->communicator = $communicator;
        $this->types = [];
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
    
    public static function getByComplexObjectType(ComplexObjectType $complexObjectType): ?array
    {
        return BinaryTypeStorage::getComplexObjectTypes()->get($complexObjectType, null);
    }

    public static function setByComplexObjectType(ComplexObjectType $complexObjectType, BinaryType $type, BinarySchema $schema): void
    {
        if (!BinaryTypeStorage::getComplexObjectTypes()->hasKey($complexObjectType)) {
            BinaryTypeStorage::getComplexObjectTypes()->put($complexObjectType, [$type, $schema]);
        }
    }

    private static function getComplexObjectTypes(): Map
    {
        if (!BinaryTypeStorage::$complexObjectTypes) {
            BinaryTypeStorage::$complexObjectTypes = new Map();
        }
        return BinaryTypeStorage::$complexObjectTypes;
    }

    private function getBinaryType(int $typeId): ?BinaryType
    {
        $binaryType = new BinaryType(null);
        $binaryType->setId($typeId);
        $this->communicator->send(
            ClientOperation::GET_BINARY_TYPE,
            function (MessageBuffer $payload) use ($typeId)
            {
                $payload->writeInteger($typeId);
            },
            function (MessageBuffer $payload) use (&$binaryType)
            {
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
        if (!$binaryType->isValid()) {
            BinaryUtils::serializationError(true, sprintf('type "%d" can not be registered', $binaryType->getId()));
        }
        $this->communicator->send(
            ClientOperation::PUT_BINARY_TYPE,
            function (MessageBuffer $payload) use ($binaryType)
            {
                $binaryType->write($payload);
            });
    }

    private function getStorageType(int $typeId): ?BinaryType
    {
        return array_key_exists($typeId, $this->types) ? $this->types[$typeId] : null;
    }
}
