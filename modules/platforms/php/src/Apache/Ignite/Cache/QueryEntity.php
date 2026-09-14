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

namespace Apache\Ignite\Cache;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\MessageBuffer;

/**
 * Class representing one Query Entity element of Ignite CacheConfiguration.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class QueryEntity
{
    private $keyTypeName;
    private $valueTypeName;
    private $tableName;
    private $keyFieldName;
    private $valueFieldName;
    private $fields;
    private $aliases;
    private $indexes;

    /**
     * QueryEntity constructor.
     */
    public function __construct()
    {
        $this->keyTypeName = null;
        $this->valueTypeName = null;
        $this->tableName = null;
        $this->keyFieldName = null;
        $this->valueFieldName = null;
        $this->fields = null;
        $this->aliases = null;
        $this->indexes = null;
    }

    /**
     *
     *
     * @param string $keyTypeName
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setKeyTypeName(string $keyTypeName): QueryEntity
    {
        $this->keyTypeName = $keyTypeName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getKeyTypeName(): ?string
    {
        return $this->keyTypeName;
    }

    /**
     *
     *
     * @param string $valueTypeName
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setValueTypeName(string $valueTypeName): QueryEntity
    {
        $this->valueTypeName = $valueTypeName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getValueTypeName(): ?string
    {
        return $this->valueTypeName;
    }

    /**
     *
     *
     * @param string $tableName
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setTableName(string $tableName): QueryEntity
    {
        $this->tableName = $tableName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getTableName(): ?string
    {
        return $this->tableName;
    }

    /**
     *
     *
     * @param string $keyFieldName
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setKeyFieldName(string $keyFieldName): QueryEntity
    {
        $this->keyFieldName = $keyFieldName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getKeyFieldName(): ?string
    {
        return $this->keyFieldName;
    }

    /**
     *
     *
     * @param string $valueFieldName
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setValueFieldName(string $valueFieldName): QueryEntity
    {
        $this->valueFieldName = $valueFieldName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getValueFieldName(): ?string
    {
        return $this->valueFieldName;
    }

    /**
     *
     *
     * @param QueryField ...$fields
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setFields(QueryField ...$fields): QueryEntity
    {
        $this->fields = $fields;
        return $this;
    }

    /**
     *
     *
     * @return QueryField[]|null
     */
    public function getFields(): ?array
    {
        return $this->fields;
    }

    /**
     *
     *
     * @param array[string => string] $aliases
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setAliases(array $aliases): QueryEntity
    {
        $this->aliases = $aliases;
        return $this;
    }

    /**
     *
     *
     * @return array[string => string]|null
     */
    public function getAliases(): ?array
    {
        return $this->aliases;
    }

    /**
     *
     *
     * @param QueryIndex ...$indexes
     *
     * @return QueryEntity the same instance of the QueryEntity.
     */
    public function setIndexes(QueryIndex ...$indexes): QueryEntity
    {
        $this->indexes = $indexes;
        return $this;
    }

    /**
     *
     *
     * @return QueryIndex[]|null
     */
    public function getIndexes(): ?array
    {
        return $this->indexes;
    }

    private function writeAliases(MessageBuffer $buffer): void
    {
        $length = $this->aliases ? count($this->aliases) : 0;
        $buffer->writeInteger($length);
        if ($length > 0) {
            foreach ($this->aliases as $key => $value) {
                BinaryCommunicator::writeString($buffer, $key);
                BinaryCommunicator::writeString($buffer, $value);
            }
        }
    }

    private function writeSubEntities(BinaryCommunicator $communicator, MessageBuffer $buffer, ?array $entities): void
    {
        $length = $entities ? count($entities) : 0;
        $buffer->writeInteger($length);
        if ($length > 0) {
            foreach ($entities as $entity) {
                $entity->write($communicator, $buffer);
            }
        }
    }

    private function readAliases(MessageBuffer $buffer): void
    {
        $length = $buffer->readInteger();
        $this->aliases = [];
        if ($length > 0) {
            for ($i = 0; $i < $length; $i++) {
                $this->aliases[BinaryCommunicator::readString($buffer)] = BinaryCommunicator::readString($buffer);
            }
        }
    }

    private function readSubEntities(BinaryCommunicator $communicator, MessageBuffer $buffer, string $subEntityClassName): array
    {
        $result = [];
        $length = $buffer->readInteger();
        if ($length > 0) {
            for ($i = 0; $i < $length; $i++) {
                $entity = new $subEntityClassName();
                $entity->read($communicator, $buffer);
                array_push($result, $entity);
            }
        }
        return $result;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        BinaryCommunicator::writeString($buffer, $this->keyTypeName);
        BinaryCommunicator::writeString($buffer, $this->valueTypeName);
        BinaryCommunicator::writeString($buffer, $this->tableName);
        BinaryCommunicator::writeString($buffer, $this->keyFieldName);
        BinaryCommunicator::writeString($buffer, $this->valueFieldName);
        $this->writeSubEntities($communicator, $buffer, $this->fields);
        $this->writeAliases($buffer);
        $this->writeSubEntities($communicator, $buffer, $this->indexes);
    }

    // This is not the public API method, is not intended for usage by an application.
    public function read(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        $this->keyTypeName = BinaryCommunicator::readString($buffer);
        $this->valueTypeName = BinaryCommunicator::readString($buffer);
        $this->tableName = BinaryCommunicator::readString($buffer);
        $this->keyFieldName = BinaryCommunicator::readString($buffer);
        $this->valueFieldName = BinaryCommunicator::readString($buffer);
        $this->fields = $this->readSubEntities($communicator, $buffer, QueryField::class);
        $this->readAliases($buffer);
        $this->indexes = $this->readSubEntities($communicator, $buffer, QueryIndex::class);
    }
}
