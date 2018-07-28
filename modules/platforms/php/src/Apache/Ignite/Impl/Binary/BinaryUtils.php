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
use Ds\Set;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\ObjectType\ObjectType;
use Apache\Ignite\ObjectType\MapObjectType;
use Apache\Ignite\ObjectType\CollectionObjectType;

class BinaryUtils
{
    public static function getSize(int $typeCode): int
    {
        return TypeInfo::getTypeInfo($typeCode)->getSize();
    }
    
    public static function checkCompatibility($value, $type): void
    {
        // TODO
    }
    
    public static function checkTypesComatibility($expectedType, int $actualTypeCode): void
    {
        // TODO
    }

    public static function calcObjectType($object)
    {
        if ($object === null) {
            return ObjectType::NULL;
        } else if (is_integer($object)) {
            return ObjectType::INTEGER;
        } else if (is_float($object)) {
            return ObjectType::DOUBLE;
        } else if (is_string($object)) {
            return ObjectType::STRING;
        } else if (is_bool($object)) {
            return ObjectType::BOOLEAN;
        } else if (is_array($object)) {
            if (count($object) > 0) {
                $values = array_values($object);
                $firstElem = $values[0];
                if ($firstElem !== null) {
                    if ($values === $object) {
                        // sequential array
                        return BinaryUtils::getArrayType(BinaryUtils::calcObjectType($firstElem));
                    } else {
                        // associative array
                        return new MapObjectType();
                    }
                }
            }
        } else if ($object instanceof DateTime) {
            return ObjectType::DATE;
        } else if ($object instanceof Set) {
            return new CollectionObjectType(CollectionObjectType::HASH_SET);
        } else if ($object instanceof Map) {
            return new MapObjectType();
        }
        throw ClientException::unsupportedTypeException(gettype($object));
    }
    
    public static function getArrayType($elementType)
    {
        switch (BinaryUtils::getTypeCode($elementType)) {
            case ObjectType::BYTE:
                return ObjectType::BYTE_ARRAY;
            case ObjectType::SHORT:
                return ObjectType::SHORT_ARRAY;
            case ObjectType::INTEGER:
                return ObjectType::INTEGER_ARRAY;
            case ObjectType::LONG:
                return ObjectType::LONG_ARRAY;
            case ObjectType::FLOAT:
                return ObjectType::FLOAT_ARRAY;
            case ObjectType::DOUBLE:
                return ObjectType::DOUBLE_ARRAY;
            case ObjectType::CHAR:
                return ObjectType::CHAR_ARRAY;
            case ObjectType::BOOLEAN:
                return ObjectType::BOOLEAN_ARRAY;
            case ObjectType::STRING:
                return ObjectType::STRING_ARRAY;
            case ObjectType::UUID:
                return ObjectType::UUID_ARRAY;
            case ObjectType::DATE:
                return ObjectType::DATE_ARRAY;
            case ObjectType::ENUM:
                return ObjectType::ENUM_ARRAY;
            case ObjectType::DECIMAL:
                return ObjectType::DECIMAL_ARRAY;
            case ObjectType::TIMESTAMP:
                return ObjectType::TIMESTAMP_ARRAY;
            case ObjectType::TIME:
                return ObjectType::TIME_ARRAY;
            default:
                throw ClientException::unsupportedTypeException(BinaryUtils::getTypeName($elementType));
        }
    }
    
    public static function getArrayElementType($arrayType)
    {
//        if ($arrayType instanceof ObjectArrayType) {
//            return $arrayType->elementType;
//        } else if ($arrayType === ObjectType::OBJECT_ARRAY) {
//            return null;
//        }
        $info = TypeInfo::getTypeInfo($arrayType);
        if (!$info || !$info->getElementTypeCode()) {
            throw ClientException::internalError();
        }
        return $info->getElementTypeCode();
    }
    
    public static function getTypeCode($objectType): int
    {
        return $objectType instanceof ObjectType ? $objectType->getTypeCode() : $objectType;
    }
    
    public static function getTypeName($objectType): string
    {
        if (is_string($objectType)) {
            return $objectType;
        }
        $typeCode = BinaryUtils::getTypeCode($objectType);
        $info = TypeInfo::getTypeInfo($typeCode);
        return $info ? $info->getName() : 'type code ' . $typeCode;
    }
    
    public static function hashCode($str)
    {
        $hash = 0;
        $length = strlen($str);
        if ($str && $length > 0) {
            for ($i = 0; $i < $length; $i++) {
                $hash = (($hash << 5) - $hash) + ord($str[$i]);
            }
        }
        return $hash;
    }
}
