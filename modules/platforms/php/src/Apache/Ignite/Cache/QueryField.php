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

namespace Apache\Ignite\Cache;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Type\ObjectType;

/**
 * Class representing one Query Field element of QueryEntity of Ignite CacheConfiguration.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class QueryField
{
    private $name;
    private $typeName;
    private $isKeyField;
    private $isNotNull;
    private $defaultValue;
    private $precision;
    private $scale;
    private $valueType;
    private $buffer;
    private $communicator;
    private $index;

    /**
     * QueryField constructor.
     *
     * @param string|null $name
     * @param string|null $typeName
     */
    public function __construct(string $name = null, string $typeName = null)
    {
        $this->name = $name;
        $this->typeName = $typeName;
        $this->isKeyField = false;
        $this->isNotNull = false;
        $this->defaultValue = null;
        $this->precision = -1;
        $this->scale = -1;
        $this->valueType = null;
        $this->buffer = null;
        $this->communicator = null;
        $this->index = null;
    }

    /**
     *
     *
     * @param string $name
     *
     * @return QueryField the same instance of the QueryField.
     */
    public function setName(string $name): QueryField
    {
        $this->name = $name;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getName(): ?string
    {
        return $this->name;
    }

    /**
     *
     *
     * @param string $typeName
     *
     * @return QueryField the same instance of the QueryField.
     */
    public function setTypeName(string $typeName): QueryField
    {
        $this->typeName = $typeName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getTypeName(): ?string
    {
        return $this->typeName;
    }

    /**
     *
     *
     * @param bool $isKeyField
     *
     * @return QueryField the same instance of the QueryField.
     */
    public function setIsKeyField(bool $isKeyField): QueryField
    {
        $this->isKeyField = $isKeyField;
        return $this;
    }

    /**
     *
     *
     * @return bool
     */
    public function getIsKeyField(): bool
    {
        return $this->isKeyField;
    }

    /**
     *
     *
     * @param bool $isNotNull
     *
     * @return QueryField the same instance of the QueryField.
     */
    public function setIsNotNull(bool $isNotNull): QueryField
    {
        $this->isNotNull = $isNotNull;
        return $this;
    }

    /**
     *
     *
     * @return bool
     */
    public function getIsNotNull(): bool
    {
        return $this->isNotNull;
    }

    /**
     *
     *
     * @param mixed $defaultValue
     * @param int|ObjectType|null $valueType type of the default value:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * @return QueryField the same instance of the QueryField.
     */
    public function setDefaultValue($defaultValue, $valueType = null): QueryField
    {
        $this->defaultValue = $defaultValue;
        $this->valueType = $valueType;
        return $this;
    }

    /**
     *
     *
     * @param int|ObjectType|null $valueType type of the default value:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return mixed
     *
     * @throws ClientException if error.
     */
    public function getDefaultValue($valueType = null)
    {
        if ($this->defaultValue === null) {
            if ($this->buffer) {
                $position = $this->buffer->getPosition();
                $this->buffer->setPosition($this->index);
                $result = $this->communicator->readObject($this->buffer, $valueType);
                $this->buffer->setPosition($position);
                return $result;
            } else {
                return null;
            }
        } else {
            return $this->defaultValue;
        }
    }

    /**
     *
     *
     * @param int $precision
     *
     * @return QueryField the same instance of the QueryField.
     */
    public function setPrecision(int $precision): QueryField
    {
        $this->precision = $precision;
        return $this;
    }

    /**
     *
     *
     * @return int
     */
    public function getPrecision(): int
    {
        return $this->precision;
    }

    /**
     *
     *
     * @param int $scale
     *
     * @return QueryField the same instance of the QueryField.
     */
    public function setScale(int $scale): QueryField
    {
        $this->scale = $scale;
        return $this;
    }

    /**
     *
     *
     * @return int
     */
    public function getScale(): int
    {
        return $this->scale;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        BinaryCommunicator::writeString($buffer, $this->name);
        BinaryCommunicator::writeString($buffer, $this->typeName);
        $buffer->writeBoolean($this->isKeyField);
        $buffer->writeBoolean($this->isNotNull);
        $communicator->writeObject($buffer, $this->defaultValue, $this->valueType);
        $buffer->writeInteger($this->precision);
        $buffer->writeInteger($this->scale);
    }

    // This is not the public API method, is not intended for usage by an application.
    public function read(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        $this->name = BinaryCommunicator::readString($buffer);
        $this->typeName = BinaryCommunicator::readString($buffer);
        $this->isKeyField = $buffer->readBoolean();
        $this->isNotNull = $buffer->readBoolean();
        $this->defaultValue = null;
        $this->communicator = $communicator;
        $this->buffer = $buffer;
        $this->index = $buffer->getPosition();
        $communicator->readObject($buffer);
        $this->precision = $buffer->readInteger();
        $this->scale = $buffer->readInteger();
    }
}
