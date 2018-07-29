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
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;

class BinaryReader
{
    public static function readObject(MessageBuffer $buffer, $expectedType = null)
    {
        $typeCode = $buffer->readByte();
        BinaryUtils::checkTypesComatibility($expectedType, $typeCode);
        return BinaryReader::readTypedObject($buffer, $typeCode, $expectedType);
    }
    
    private static function readTypedObject(MessageBuffer $buffer, int $objectTypeCode, $expectedType = null)
    {
        switch ($objectTypeCode) {
            case ObjectType::BYTE:
            case ObjectType::SHORT:
            case ObjectType::INTEGER:
            case ObjectType::LONG:
            case ObjectType::FLOAT:
            case ObjectType::DOUBLE:
                return $buffer->readNumber($objectTypeCode);
            case ObjectType::CHAR:
                return $buffer->readChar();
            case ObjectType::BOOLEAN:
                return $buffer->readBoolean();
            case ObjectType::STRING:
                return $buffer->readString();
            case ObjectType::DATE:
                return BinaryReader::readDate($buffer);
            case ObjectType::TIME:
                return BinaryReader::readTime($buffer);
            case ObjectType::TIMESTAMP:
                return BinaryReader::readTimestamp($buffer);
            case ObjectType::BYTE_ARRAY:
            case ObjectType::SHORT_ARRAY:
            case ObjectType::INTEGER_ARRAY:
            case ObjectType::LONG_ARRAY:
            case ObjectType::FLOAT_ARRAY:
            case ObjectType::DOUBLE_ARRAY:
            case ObjectType::CHAR_ARRAY:
            case ObjectType::BOOLEAN_ARRAY:
            case ObjectType::STRING_ARRAY:
            case ObjectType::UUID_ARRAY:
            case ObjectType::DATE_ARRAY:
            case ObjectType::OBJECT_ARRAY:
            case ObjectType::ENUM_ARRAY:
            case ObjectType::DECIMAL_ARRAY:
            case ObjectType::TIMESTAMP_ARRAY:
            case ObjectType::TIME_ARRAY:
                return BinaryReader::readArray($buffer, $objectTypeCode, $expectedType);
            case ObjectType::MAP:
                return BinaryReader::readMap($buffer, $expectedType);
            case ObjectType::NULL:
                return null;
            default:
                BinaryUtils::unsupportedType($objectTypeCode);
        }
    }
    
    private static function readDate(MessageBuffer $buffer): Date {
        return new Date($buffer->readLong());
    }

    private static function readTime(MessageBuffer $buffer): Time {
        return new Time($buffer->readLong());
    }

    private static function readTimestamp(MessageBuffer $buffer): Timestamp {
        return new Timestamp($buffer->readLong(), $buffer->readInteger());
    }
    
    private static function readArray(MessageBuffer $buffer, int $arrayTypeCode, $arrayType): array
    {
        if ($arrayTypeCode === ObjectType::OBJECT_ARRAY) {
            $buffer->readInteger();
        }
        $length = $buffer->readInteger();
        $elementType = BinaryUtils::getArrayElementType($arrayType ? $arrayType : $arrayTypeCode);
        $keepElementType = $elementType === null ? true : TypeInfo::getTypeInfo(BinaryUtils::getTypeCode($elementType))->isNullable();
        $result = array();
        for ($i = 0; $i < $length; $i++) {
            array_push(
                $result,
                $keepElementType ?
                    BinaryReader::readObject($buffer, $elementType) :
                    BinaryReader::readTypedObject($buffer, $elementType));
        }
        return $result;
    }
    
    private static function readMap(MessageBuffer $buffer, MapObjectType $expectedMapType = null): Map
    {
        $size = $buffer->readInteger();
        // map sub-type
        $buffer->readByte();
        $result = new Map();
        $result->allocate($size);
        for ($i = 0; $i < $size; $i++) {
            $key = BinaryReader::readObject($buffer, $expectedMapType ? $expectedMapType->getKeyType() : null);
            $value = BinaryReader::readObject($buffer, $expectedMapType ? $expectedMapType->getValueType() : null);
            $result->put($key, $value);
        }
        return $result;
    }
}
