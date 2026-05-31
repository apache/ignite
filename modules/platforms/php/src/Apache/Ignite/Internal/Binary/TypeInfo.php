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

class TypeInfo
{
    const NAME = 'name';
    const SIZE = 'size';
    const MIN_VALUE = 'min';
    const MAX_VALUE = 'max';
    const MAX_UNSIGNED_VALUE = 'max_unsigned';
    const NULLABLE = 'nullable';
    const ELEMENT_TYPE_CODE = 'element_type';
    const KEEP_ELEMENT_TYPE = 'keep_element_type';
    const MAX_INT_VALUE = 2147483647;

    private $properties;
    
    private static $info;
    private static $primitiveTypes;
    
    public static function init(): void
    {
        TypeInfo::$info = array(
            ObjectType::BYTE => new TypeInfo([
                TypeInfo::NAME => 'byte',
                TypeInfo::SIZE => 1,
                TypeInfo::MIN_VALUE => -128,
                TypeInfo::MAX_VALUE => 127,
                TypeInfo::MAX_UNSIGNED_VALUE => 0x100,
            ]),
            ObjectType::SHORT => new TypeInfo([
                TypeInfo::NAME => 'short',
                TypeInfo::SIZE => 2,
                TypeInfo::MIN_VALUE => -32768,
                TypeInfo::MAX_VALUE => 32767,
                TypeInfo::MAX_UNSIGNED_VALUE => 0x10000,
            ]),
            ObjectType::INTEGER => new TypeInfo([
                TypeInfo::NAME => 'integer',
                TypeInfo::SIZE => 4,
                TypeInfo::MIN_VALUE => -2147483648,
                TypeInfo::MAX_VALUE => TypeInfo::MAX_INT_VALUE,
            ]),
            ObjectType::LONG => new TypeInfo([
                TypeInfo::NAME => 'long',
                TypeInfo::SIZE => 8,
            ]),
            ObjectType::FLOAT => new TypeInfo([
                TypeInfo::NAME => 'float',
                TypeInfo::SIZE => 4,
            ]),
            ObjectType::DOUBLE => new TypeInfo([
                TypeInfo::NAME => 'double',
                TypeInfo::SIZE => 8,
            ]),
            ObjectType::CHAR => new TypeInfo([
                TypeInfo::NAME => 'char',
                TypeInfo::SIZE => 2,
            ]),
            ObjectType::BOOLEAN => new TypeInfo([
                TypeInfo::NAME => 'boolean',
                TypeInfo::SIZE => 1,
            ]),
            ObjectType::STRING => new TypeInfo([
                TypeInfo::NAME => 'string',
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::UUID => new TypeInfo([
                TypeInfo::NAME => 'UUID',
                TypeInfo::SIZE => 16,
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::DATE => new TypeInfo([
                TypeInfo::NAME => 'date',
                TypeInfo::SIZE => 8,
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::BYTE_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'byte array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::BYTE
            ]),
            ObjectType::SHORT_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'short array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::SHORT
            ]),
            ObjectType::INTEGER_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'integer array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::INTEGER
            ]),
            ObjectType::LONG_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'long array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::LONG
            ]),
            ObjectType::FLOAT_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'float array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::FLOAT
            ]),
            ObjectType::DOUBLE_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'double array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::DOUBLE
            ]),
            ObjectType::CHAR_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'char array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::CHAR
            ]),
            ObjectType::BOOLEAN_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'boolean array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::BOOLEAN
            ]),
            ObjectType::STRING_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'string array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::STRING,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::UUID_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'UUID array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::UUID,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::DATE_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'date array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::DATE,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::OBJECT_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'object array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::COMPLEX_OBJECT,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::COLLECTION => new TypeInfo([
                TypeInfo::NAME => 'collection',
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::MAP => new TypeInfo([
                TypeInfo::NAME => 'map',
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::ENUM => new TypeInfo([
                TypeInfo::NAME => 'enum',
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::ENUM_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'enum array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::ENUM,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::DECIMAL => new TypeInfo([
                TypeInfo::NAME => 'decimal',
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::DECIMAL_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'decimal array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::DECIMAL,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::TIMESTAMP => new TypeInfo([
                TypeInfo::NAME => 'timestamp',
                TypeInfo::SIZE => 12,
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::TIMESTAMP_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'timestamp array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::TIMESTAMP,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::TIME => new TypeInfo([
                TypeInfo::NAME => 'time',
                TypeInfo::SIZE => 8,
                TypeInfo::NULLABLE => true,
            ]),
            ObjectType::TIME_ARRAY => new TypeInfo([
                TypeInfo::NAME => 'time array',
                TypeInfo::NULLABLE => true,
                TypeInfo::ELEMENT_TYPE_CODE => ObjectType::TIME,
                TypeInfo::KEEP_ELEMENT_TYPE => true
            ]),
            ObjectType::NULL => new TypeInfo([
                TypeInfo::NAME => 'null',
                TypeInfo::NULLABLE => true,
            ])
        );
        
        TypeInfo::$primitiveTypes = [
            ObjectType::BYTE,
            ObjectType::SHORT,
            ObjectType::INTEGER,
            ObjectType::LONG,
            ObjectType::FLOAT,
            ObjectType::DOUBLE,
            ObjectType::CHAR,
            ObjectType::BOOLEAN,
            ObjectType::STRING,
            ObjectType::UUID,
            ObjectType::DATE,
            ObjectType::BYTE_ARRAY,
            ObjectType::SHORT_ARRAY,
            ObjectType::INTEGER_ARRAY,
            ObjectType::LONG_ARRAY,
            ObjectType::FLOAT_ARRAY,
            ObjectType::DOUBLE_ARRAY,
            ObjectType::CHAR_ARRAY,
            ObjectType::BOOLEAN_ARRAY,
            ObjectType::STRING_ARRAY,
            ObjectType::UUID_ARRAY,
            ObjectType::DATE_ARRAY,
            ObjectType::ENUM,
            ObjectType::ENUM_ARRAY,
            ObjectType::DECIMAL,
            ObjectType::DECIMAL_ARRAY,
            ObjectType::TIMESTAMP,
            ObjectType::TIMESTAMP_ARRAY,
            ObjectType::TIME,
            ObjectType::TIME_ARRAY
        ];
    }

    public static function getTypeInfo(int $typeCode): ?TypeInfo
    {
        return array_key_exists($typeCode, TypeInfo::$info) ? TypeInfo::$info[$typeCode] : null;
    }
    
    public static function getPrimitiveTypes(): array
    {
        return TypeInfo::$primitiveTypes;
    }
    
    private function __construct(array $properties)
    {
        $this->properties = $properties;
    }
    
    public function getName(): string
    {
        return $this->getProperty(TypeInfo::NAME, null);
    }

    public function getSize(): int
    {
        return $this->getProperty(TypeInfo::SIZE, 0);
    }
    
    public function isNullable(): bool
    {
        return $this->getProperty(TypeInfo::NULLABLE, false);
    }
    
    public function getElementTypeCode(): int
    {
        return $this->getProperty(TypeInfo::ELEMENT_TYPE_CODE, 0);
    }
    
    public function keepElementType(): bool
    {
        return $this->getProperty(TypeInfo::KEEP_ELEMENT_TYPE, false);
    }
    
    public function getMinValue()
    {
        return $this->getProperty(TypeInfo::MIN_VALUE, null);
    }
    
    public function getMaxValue()
    {
        return $this->getProperty(TypeInfo::MAX_VALUE, null);
    }

    public function getMaxUnsignedValue()
    {
        return $this->getProperty(TypeInfo::MAX_UNSIGNED_VALUE, null);
    }

    private function getProperty(string $propName, $defaultValue)
    {
        return array_key_exists($propName, $this->properties) ? $this->properties[$propName] : $defaultValue;
    }
}

TypeInfo::init();
