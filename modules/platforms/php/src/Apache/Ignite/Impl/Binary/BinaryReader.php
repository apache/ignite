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
use Brick\Math\BigDecimal;
use Brick\Math\BigInteger;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Type\CollectionObjectType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;
use Apache\Ignite\Data\EnumItem;
use Apache\Ignite\Data\BinaryObject;

class BinaryReader
{
    public static function readObject(MessageBuffer $buffer, $expectedType = null)
    {
        $typeCode = $buffer->readByte();
        BinaryUtils::checkTypesComatibility($expectedType, $typeCode);
        return BinaryReader::readTypedObject($buffer, $typeCode, $expectedType);
    }
    
    public static function readStringArray(MessageBuffer $buffer): array
    {
        return BinaryReader::readTypedObject($buffer, ObjectType::STRING_ARRAY);
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
            case ObjectType::UUID:
                return BinaryReader::readUUID($buffer);
            case ObjectType::DATE:
                return BinaryReader::readDate($buffer);
            case ObjectType::ENUM:
            case ObjectType::BINARY_ENUM:
                return BinaryReader::readEnum($buffer);
            case ObjectType::DECIMAL:
                return BinaryReader::readDecimal($buffer);
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
            case ObjectType::COLLECTION:
                return BinaryReader::readCollection($buffer, $expectedType);
            case ObjectType::MAP:
                return BinaryReader::readMap($buffer, $expectedType);
            case ObjectType::BINARY_OBJECT:
                return BinaryReader::readBinaryObject($buffer, $expectedType);
            case ObjectType::NULL:
                return null;
            case ObjectType::COMPLEX_OBJECT:
                return BinaryReader::readComplexObject($buffer, $expectedType);
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
    
    private static function readUUID(MessageBuffer $buffer): array
    {
        $result = [];
        for ($i = 0; $i < BinaryUtils::getSize(ObjectType::UUID); $i++) {
            array_push($result, $buffer->readByte(false));
        }
        return $result;
    }
    
    private static function readEnum(MessageBuffer $buffer): EnumItem
    {
        $enumItem = new EnumItem($buffer->readInteger());
        $ordinal = $buffer->readInteger();
        $enumItem->setOrdinal($ordinal);
        $type = BinaryTypeStorage::getEntity()->getType($enumItem->getTypeId());
        if (!$type->isEnum() || !$type->getEnumValues() || count($type->getEnumValues()) <= $ordinal) {
            BinaryUtils::serializationError(false, 'EnumItem can not be deserialized: type mismatch');
        }
        $enumValues = $type->getEnumValues();
        $enumItem->setName($enumValues[$ordinal][0]);
        $enumItem->setValue($enumValues[$ordinal][1]);
        return $enumItem;
    }

    private static function readDecimal(MessageBuffer $buffer): BigDecimal
    {
        $scale = $buffer->readInteger();
        $value = $buffer->readString();
        $isNegative = (ord($value[0]) & 0x80) !== 0;
        if ($isNegative) {
            $value[0] = chr(ord($value[0]) & 0x7F);
        }
        $hexValue = '';
        for ($i = 0; $i < strlen($value); $i++) {
            $hex = dechex(ord($value[$i]));
            if (strlen($hex) < 2) {
                $hex = str_repeat('0', 2 - strlen($hex)) . $hex;
            }
            $hexValue .= $hex;
        }
        $result = BigDecimal::ofUnscaledValue(BigInteger::parse($hexValue, 16), $scale >= 0 ? $scale : 0);
        if ($isNegative) {
            $result = $result->negated();
        }
        if ($scale < 0) {
            $result = $result->multipliedBy((new BigInteger(10))->power(-$scale));
        }
        return $result;
    }
    
    private static function readArray(MessageBuffer $buffer, int $arrayTypeCode, $arrayType): array
    {
        if ($arrayTypeCode === ObjectType::OBJECT_ARRAY) {
            $buffer->readInteger();
        }
        $length = $buffer->readInteger();
        $elementType = BinaryUtils::getArrayElementType($arrayType ? $arrayType : $arrayTypeCode);
        $keepElementType = $elementType === null ? true : TypeInfo::getTypeInfo($arrayTypeCode)->keepElementType();
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
    
    private static function readCollection(MessageBuffer $buffer, CollectionObjectType $expectedColType = null)
    {
        $size = $buffer->readInteger();
        $subType = $buffer->readByte();
        $isSet = CollectionObjectType::isSet($subType);
        $result = $isSet ? new Set() : [];
        for ($i = 0; $i < $size; $i++) {
            $element = BinaryReader::readObject($buffer, $expectedColType ? $expectedColType->getElementType() : null);
            if ($isSet) {
                $result->add($element);
            } else {
                array_push($result, $element);
            }
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
    
    private static function readBinaryObject(MessageBuffer $buffer, ?ComplexObjectType $expectedType): object
    {
        $size = $buffer->readInteger();
        $startPos = $buffer->getPosition();
        $buffer->setPosition($startPos + $size);
        $offset = $buffer->readInteger();
        $endPos = $buffer->getPosition();
        $buffer->setPosition($startPos + $offset);
        $result = BinaryReader::readObject($buffer, $expectedType);
        $buffer->setPosition($endPos);
        return $result;
    }

    private static function readComplexObject(MessageBuffer $buffer, ?ComplexObjectType $expectedType): object
    {
        $buffer->setPosition($buffer->getPosition() - 1);
        $binaryObject = BinaryObject::fromBuffer($buffer);
        return $expectedType ? $binaryObject->toObject($expectedType) : $binaryObject;
    }
}
