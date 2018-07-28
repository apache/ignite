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
use Apache\Ignite\Exception\IgniteClientException;
use Apache\Ignite\ObjectType\ObjectType;

class BinaryWriter
{
    public static function writeString(MessageBuffer $buffer, ?string $value): void
    {
        BinaryWriter::writeObject($buffer, $value, ObjectType::STRING);
    }

    public static function writeObject(MessageBuffer $buffer, $object, $objectType = null, bool $writeObjectType = true): void
    {
        BinaryUtils::checkCompatibility($object, $objectType);
        if ($object === null) {
            $buffer->writeByte(ObjectType::NULL);
            return;
        }

        $objectType = $objectType ? $objectType : BinaryUtils::calcObjectType($object);
        $objectTypeCode = BinaryUtils::getTypeCode($objectType);

        if ($writeObjectType) {
            $buffer->writeByte($objectTypeCode);
        }
        switch ($objectTypeCode) {
            case ObjectType::BYTE:
            case ObjectType::SHORT:
            case ObjectType::INTEGER:
            case ObjectType::LONG:
            case ObjectType::FLOAT:
            case ObjectType::DOUBLE:
                $buffer->writeNumber($object, $objectTypeCode);
                break;
            case ObjectType::CHAR:
                $buffer->writeChar($object);
                break;
            case ObjectType::BOOLEAN:
                $buffer->writeBoolean($object);
                break;
            case ObjectType::STRING:
                $buffer->writeString($object);
                break;
            case ObjectType::DATE:
                $buffer->writeDate($object);
                break;
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
                BinaryWriter::writeArray($buffer, $object, $objectType, $objectTypeCode);
                break;
            case ObjectType::MAP:
                BinaryWriter::writeMap($buffer, $object, $objectType);
                break;
            default:
                throw IgniteClientException::unsupportedTypeException($objectType);
        }
    }
    
    private static function writeArray(MessageBuffer $buffer, array $array, $arrayType, int $arrayTypeCode): void
    {
        $elementType = BinaryUtils::getArrayElementType($arrayType);
        $keepElementType = !$elementType || TypeInfo::getTypeInfo(BinaryUtils::getTypeCode($elementType))->isNullable();
        $buffer->writeInteger(count($array));
        foreach ($array as $elem) {
            BinaryWriter::writeObject($buffer, $elem, $elementType, $keepElementType);
        }
    }
    
    private static function writeMap(MessageBuffer $buffer, $map, MapObjectType $mapType): void
    {
        if ($map instanceof Map) {
            $buffer->writeInteger($map->count);
            $buffer->writeByte($mapType->getSubType());
            foreach ($map->pairs() as $pair) {
                BinaryWriter::writeObject($buffer, $pair->key, $mapType->getKeyType());
                BinaryWriter::writeObject($buffer, $pair->value, $mapType->getValueType());
            }
        }
        else {
            $buffer->writeInteger(count($map));
            $buffer->writeByte($mapType->getSubType());
            foreach ($map as $key => $value) {
                BinaryWriter::writeObject($buffer, $key, $mapType->getKeyType());
                BinaryWriter::writeObject($buffer, $value, $mapType->getValueType());
            }
        }
    }
}
