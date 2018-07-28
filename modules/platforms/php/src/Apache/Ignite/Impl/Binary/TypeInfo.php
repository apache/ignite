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

use Apache\Ignite\ObjectType\ObjectType;

class TypeInfo
{
    private $name;
    private $size;
    private $nullable;
    private $elementTypeCode;
    
    private static $info;
    
    static function init(): void
    {
        TypeInfo::$info = array(
            ObjectType::BYTE => new TypeInfo('byte', 1),
            ObjectType::SHORT => new TypeInfo('short', 2),
            ObjectType::INTEGER => new TypeInfo('integer', 4),
            ObjectType::LONG => new TypeInfo('long', 8),
            ObjectType::FLOAT => new TypeInfo('float', 4),
            ObjectType::DOUBLE => new TypeInfo('double', 8),
            ObjectType::CHAR => new TypeInfo('char', 2),
            ObjectType::BOOLEAN => new TypeInfo('boolean', 1),
            ObjectType::STRING => new TypeInfo('string', 0, true),
            ObjectType::DATE => new TypeInfo('date', 8, true),
            ObjectType::BYTE_ARRAY => new TypeInfo('byte array', 0, true, ObjectType::BYTE),
            ObjectType::SHORT_ARRAY => new TypeInfo('short array', 0, true, ObjectType::SHORT),
            ObjectType::INTEGER_ARRAY => new TypeInfo('integer array', 0, true, ObjectType::INTEGER),
            ObjectType::LONG_ARRAY => new TypeInfo('long array', 0, true, ObjectType::LONG),
            ObjectType::FLOAT_ARRAY => new TypeInfo('float array', 0, true, ObjectType::FLOAT),
            ObjectType::DOUBLE_ARRAY => new TypeInfo('double array', 0, true, ObjectType::DOUBLE),
            ObjectType::CHAR_ARRAY => new TypeInfo('char array', 0, true, ObjectType::CHAR),
            ObjectType::BOOLEAN_ARRAY => new TypeInfo('boolean array', 0, true, ObjectType::BOOLEAN),
            ObjectType::STRING_ARRAY => new TypeInfo('string array', 0, true, ObjectType::STRING),
            ObjectType::DATE_ARRAY => new TypeInfo('date array', 0, true, ObjectType::DATE),
            ObjectType::MAP => new TypeInfo('map', 0, true),
            ObjectType::NULL => new TypeInfo('null', 0, true),
        );
    }
    
    static function getTypeInfo(int $typeCode): TypeInfo
    {
        return TypeInfo::$info[$typeCode];
    }
    
    private function __construct(string $name, int $size, bool $nullable = false, int $elementTypeCode = 0)
    {
        $this->name = $name;
        $this->size = $size;
        $this->nullable = $nullable;
        $this->elementTypeCode = $elementTypeCode;
    }
    
    public function getName(): string
    {
        return $this->name;
    }

    public function getSize(): int
    {
        return $this->size;
    }
    
    public function isNullable(): bool
    {
        return $this->nullable;
    }
    
    public function getElementTypeCode(): int
    {
        return $this->elementTypeCode;
    }
}

TypeInfo::init();
