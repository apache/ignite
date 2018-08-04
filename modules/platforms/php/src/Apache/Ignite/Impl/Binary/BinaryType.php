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

use Apache\Ignite\Impl\Binary\MessageBuffer;

class BinaryType
{
    private $name;
    private $id;
    private $fields;
    private $schemas;
    private $isEnum;
    private $enumValues;
    
    public function __construct(?string $name)
    {
        $this->name = $name;
        $this->id = BinaryType::calculateId($name);
        $this->fields = [];
        $this->schemas = [];
        $this->isEnum = false;
        $this->enumValues = null;
    }
    
    public static function calculateId($name)
    {
        return BinaryUtils::hashCodeLowerCase($name);
    }

    public function getId(): int
    {
        return $this->id;
    }
    
    public function setId(int $id): void
    {
        $this->id = $id;
    }
    
    public function getName(): string
    {
        return $this->name;
    }
    
    public function getFields(): array
    {
        return array_values($this->fields);
    }

    public function getField(int $fieldId): ?BinaryField
    {
        return $this->hasField($fieldId) ? $this->fields[$fieldId] : null;
    }

    public function hasField(int $fieldId): bool
    {
        return array_key_exists($fieldId, $this->fields);
    }

    public function removeField(int $fieldId): void
    {
        if ($this->hasField($fieldId)) {
            unset($this->fields[$fieldId]);
        }
    }

    public function setField(BinaryField $field): void
    {
        $this->fields[$field->getId()] = $field;
    }

    public function hasSchema(int $schemaId): bool
    {
        return array_key_exists($schemaId, $this->schemas);
    }

    public function addSchema(BinarySchema $schema): void
    {
        if (!$this->hasSchema($schema->getId())) {
            $this->schemas[$schema->getId()] = $schema;
        }
    }

    public function getSchema($schemaId): ?BinarySchema
    {
        return $this->hasSchema($schemaId) ? $this->schemas[$schemaId] : null;
    }
    
    public function isEnum(): bool
    {
        return $this->isEnum;
    }
    
    public function getEnumValues(): ?array
    {
        return $this->enumValues;
    }

    public function merge(BinaryType $binaryType, BinarySchema $binarySchema): void
    {
        foreach ($binaryType->getFields() as $field) {
            $fieldId = $field->getId();
            if ($this->hasField($fieldId)) {
                if ($this->getField($fieldId)->getTypeCode() !== $field->getTypeCode()) {
                    BinaryUtils::serializationError(
                        true, printf('type conflict for field "%s" of complex object type "%s"'),
                        $field->getName(), $this->name);
                }
            } else {
                $this->setField($field);
            }
        }
        $this->addSchema($binarySchema);
    }

    public function cloneType(): BinaryType
    {
        $result = new BinaryType();
        $result->name = $this->name;
        $result->id = $this->id;
        $result->fields = array($this->fields);
        $result->schemas = array($this->schemas);
        $result->isEnum = $this->isEnum;
        return result;
    }

    public function write(MessageBuffer $buffer): void
    {
        // type id
        $buffer->writeInteger($this->id);
        // type name
        BinaryWriter::writeString($buffer, $this->name);
        // affinity key field name
        BinaryWriter::writeString($buffer, null);
        // fields count
        $buffer->writeInteger(count($this->fields));
        // fields
        foreach ($this->fields as $field) {
            $field->write($buffer);
        }
        $this->writeEnum($buffer);
        // schemas count
        $buffer->writeInteger(count($this->schemas));
        foreach ($this->schemas as $schema) {
            $schema->write($buffer);
        }
    }

    private function writeEnum(MessageBuffer $buffer): void
    {
        $buffer->writeBoolean($this->isEnum);
        if ($this->isEnum) {
            $length = $this->enumValues ? count($this->enumValues) : 0;
            $buffer->writeInteger($length);
            if ($length > 0) {
                foreach ($this->enumValues as $key => $value) {
                    BinaryWriter::writeString($buffer, $key);
                    $buffer->writeInteger($value);
                }
            }
        }
    }

    public function read(MessageBuffer $buffer): void
    {
        // type id
        $this->id = $buffer->readInteger();
        // type name
        $this->name = BinaryReader::readObject($buffer);
        // affinity key field name
        BinaryReader::readObject($buffer);
        // fields count
        $fieldsCount = $buffer->readInteger();
        // fields
        for ($i = 0; $i < $fieldsCount; $i++) {
            $field = new BinaryField(null, null);
            $field->read($buffer);
            $this->setField($field);
        }
        $this->readEnum($buffer);
        // schemas count
        $schemasCount = $buffer->readInteger();
        // schemas
        for ($i = 0; $i < $schemasCount; $i++) {
            $schema = new BinarySchema();
            $schema->read($buffer);
            $this->addSchema($schema);
        }
    }

    private function readEnum(MessageBuffer $buffer): void
    {
        $this->isEnum = $buffer->readBoolean();
        if ($this->isEnum) {
            $valuesCount = $buffer->readInteger();
            $this->enumValues = [];
            for ($i = 0; $i < $valuesCount; $i++) {
                array_push($this->enumValues, [BinaryReader::readObject($buffer), $buffer->readInteger()]);
            }
        }
    }
}
