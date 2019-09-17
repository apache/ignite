<?php
/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Ignite\Internal\Binary;

class BinaryField
{
    private $name;
    private $id;
    private $typeCode;

    public function __construct(string $name = null, int $typeCode = 0)
    {
        $this->name = $name;
        $this->id = BinaryField::calculateId($name);
        $this->typeCode = $typeCode;
    }

    public static function calculateId(?string $name): int
    {
        return BinaryUtils::hashCodeLowerCase($name);
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getTypeCode(): int
    {
        return $this->typeCode;
    }

    public function isValid(): bool
    {
        return $this->name !== null;
    }

    public function write(MessageBuffer $buffer): void
    {
        // field name
        BinaryCommunicator::writeString($buffer, $this->name);
        // type code
        $buffer->writeInteger($this->typeCode);
        // field id
        $buffer->writeInteger($this->id);
    }

    public function read(MessageBuffer $buffer): void
    {
        // field name
        $this->name = BinaryCommunicator::readString($buffer);
        // type code
        $this->typeCode = $buffer->readInteger();
        // field id
        $this->id = $buffer->readInteger();
    }
}
