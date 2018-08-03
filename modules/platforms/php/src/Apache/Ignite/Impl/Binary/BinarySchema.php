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

class BinarySchema
{
    /** FNV1 hash offset basis. */
    const FNV1_OFFSET_BASIS = 0x811C9DC5;
    /** FNV1 hash prime. */
    const FNV1_PRIME = 0x01000193;
    
    private $id;
    private $fieldIds;
    private $isValid;
    
    public function __construct()
    {
        $this->id = BinarySchema::FNV1_OFFSET_BASIS;
        $this->fieldIds = [];
        $this->isValid = true;
    }
    
    public function getId(): int
    {
        return $this->id;
    }
    
    public function getFieldIds(): array
    {
        return array_keys($this->fieldIds);
    }

    public function finalize(): void
    {
        if (!$this->isValid) {
            $this->id = BinarySchema::FNV1_OFFSET_BASIS;
            foreach ($this->fieldIds as $key => $value) {
                $this->id = BinarySchema::updateSchemaId($this->id, $key);
            }
            $this->isValid = true;
        }
    }

    public function cloneSchema(): BinarySchema
    {
        $result = new BinarySchema();
        $result->id = $this->id;
        $result->fieldIds = array($this->fieldIds); new Set(this._fieldIds);
        $result->isValid = $this->isValid;
        return result;
    }
    
    public function addField(int $fieldId): void
    {
        if (!$this->hasField($fieldId)) {
            $this->fieldIds[$fieldId] = true;
            if ($this->isValid) {
                $this->updateSchemaId($fieldId);
            }
        }
    }

    public function removeField(int $fieldId): void
    {
        if ($this->hasField($fieldId)) {
            unset($this->fieldIds[$fieldId]);
            $this->isValid = false;
        }
    }

    public function hasField(int $fieldId): bool
    {
        return array_key_exists($fieldId, $this->fieldIds);
    }

    private function updateSchemaId(int $fieldId): void
    {
        $this->id = $this->id ^ ($fieldId & 0xFF);
        $this->id = $this->id * BinarySchema::FNV1_PRIME;
        $this->id &= 0xFFFFFFFF; // Convert to 32bit integer
        $this->id = $this->id ^ (($fieldId >> 8) & 0xFF);
        $this->id = $this->id * BinarySchema::FNV1_PRIME;
        $this->id &= 0xFFFFFFFF; // Convert to 32bit integer
        $this->id = $this->id ^ (($fieldId >> 16) & 0xFF);
        $this->id = $this->id * BinarySchema::FNV1_PRIME;
        $this->id &= 0xFFFFFFFF; // Convert to 32bit integer
        $this->id = $this->id ^ (($fieldId >> 24) & 0xFF);
        $this->id = $this->id * BinarySchema::FNV1_PRIME;
        $this->id &= 0xFFFFFFFF; // Convert to 32bit integer
        $this->id = BinaryUtils::intval32($this->id);
    }

    public function write(MessageBuffer $buffer): void
    {
        $this->finalize();
        // schema id
        $buffer->writeInteger($this->id);
        // fields count
        $buffer->writeInteger(count($this->fieldIds));
        // field ids
        foreach ($this->fieldIds as $key => $value) {
            $buffer->writeInteger($key);
        }
    }
    
    public function read(MessageBuffer $buffer): void
    {
        // schema id
        $this->id = $buffer->readInteger();
        // fields count
        $fieldsCount = $buffer->readInteger();
        // field ids
        for ($i = 0; $i < $fieldsCount; $i++) {
            $this->fieldIds[$buffer->readInteger()] = true;
        }
    }
}
