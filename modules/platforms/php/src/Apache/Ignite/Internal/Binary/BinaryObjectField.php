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

namespace Apache\Ignite\Internal\Binary;

use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Exception\ClientException;

class BinaryObjectField
{
    private $name;
    private $id;
    private $value;
    private $type;
    private $typeCode;

    private $communicator;
    private $buffer;
    private $offset;
    private $length;

    public function __construct(?string $name, $value = null, $type = null)
    {
        $this->name = $name;
        $this->id = BinaryField::calculateId($name);
        $this->value = $value;
        $this->type = $type;
        if (!$type && $value !== null) {
            $this->type = BinaryUtils::calcObjectType($value);
        }
        $this->typeCode = $this->type ? BinaryUtils::getTypeCode($this->type) : 0;
    }
    
    public function getId(): int
    {
        return $this->id;
    }
    
    public function getTypeCode(): int
    {
        return $this->typeCode;
    }

    public function getValue($type = null)
    {
        if ($this->buffer && ($this->value === null || $this->type !== $type)) {
            $this->buffer->setPosition($this->offset);
            $this->value = $this->communicator->readObject($this->buffer, $type);
            $this->type = $type;
        }
        return $this->value;
    }

    public static function fromBuffer(BinaryCommunicator $communicator, MessageBuffer $buffer, int $offset, int $length, int $id): BinaryObjectField
    {
        $result = new BinaryObjectField(null);
        $result->id = $id;
        $result->communicator = $communicator;
        $result->buffer = $buffer;
        $result->offset = $offset;
        $result->length = $length;
        return $result;
    }

    public function writeValue(BinaryCommunicator $communicator, MessageBuffer $buffer, int $expectedTypeCode): void
    {
        $offset = $buffer->getPosition();
        if ($this->buffer && $this->communicator === $communicator) {
            $buffer->writeBuffer($this->buffer, $this->offset, $this->length);
        } else {
            if (!$this->value) {
                $this->getValue($expectedTypeCode);
            }
            BinaryUtils::checkCompatibility($this->value, $expectedTypeCode);
            $communicator->writeObject($buffer, $this->value, $this->type);
        }
        $this->communicator = $communicator;
        $this->buffer = $buffer;
        $this->length = $buffer->getPosition() - $offset;
        $this->offset = $offset;
    }

    public function writeOffset(MessageBuffer $buffer, int $headerStartPos, int $offsetType): void
    {
        $buffer->writeNumber($this->offset - $headerStartPos, $offsetType, false);
    }

    public function getOffsetType(int $headerStartPos): int
    {
        $offset = $this->offset - $headerStartPos;
        if ($offset < TypeInfo::getTypeInfo(ObjectType::BYTE)->getMaxUnsignedValue()) {
            return ObjectType::BYTE;
        } elseif ($offset < TypeInfo::getTypeInfo(ObjectType::SHORT)->getMaxUnsignedValue()) {
            return ObjectType::SHORT;
        }
        return ObjectType::INTEGER;
    }
}
