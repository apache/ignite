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
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Type\CollectionObjectType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;
use Apache\Ignite\Data\EnumItem;
use Apache\Ignite\Data\BinaryObject;
use Apache\Ignite\Impl\Utils\ArgumentChecker;

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
            case ObjectType::UUID:
                BinaryWriter::writeUUID($buffer, $object);
                break;
            case ObjectType::DATE:
                BinaryWriter::writeDate($buffer, $object);
                break;
            case ObjectType::ENUM:
                BinaryWriter::writeEnum($buffer, $object);
                break;
            case ObjectType::DECIMAL:
                BinaryWriter::writeDecimal($buffer, $object);
                break;
            case ObjectType::TIME:
                BinaryWriter::writeTime($buffer, $object);
                break;
            case ObjectType::TIMESTAMP:
                BinaryWriter::writeTimestamp($buffer, $object);
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
            case ObjectType::COLLECTION:
                BinaryWriter::writeCollection($buffer, $object, $objectType);
                break;
            case ObjectType::MAP:
                BinaryWriter::writeMap($buffer, $object, $objectType);
                break;
            case ObjectType::BINARY_OBJECT:
                BinaryWriter::writeBinaryObject($buffer, $object, $objectType);
                break;
            case ObjectType::COMPLEX_OBJECT:
                BinaryWriter::writeComplexObject($buffer, $object, $objectType);
                break;
            default:
                BinaryUtils::unsupportedType($objectType);
        }
    }
    
    private static function writeDate(MessageBuffer $buffer, Date $date): void
    {
        $buffer->writeLong($date->getMillis());
    }

    private static function writeTime(MessageBuffer $buffer, Time $time): void
    {
        $buffer->writeLong($time->getMillis());
    }
    
    private static function writeTimestamp(MessageBuffer $buffer, Timestamp $timestamp): void
    {
        $buffer->writeLong($timestamp->getMillis());
        $buffer->writeInteger($timestamp->getNanos());
    }

    private static function writeUUID(MessageBuffer $buffer, array $value): void
    {
        for ($i = 0; $i < count($value); $i++) {
            $buffer->writeByte($value[$i], false);
        }
    }
    
    private static function writeEnum(MessageBuffer $buffer, EnumItem $enumValue): void
    {
        $buffer->writeInteger($enumValue->getTypeId());
        if ($enumValue->getOrdinal() !== null) {
            $buffer->writeInteger($enumValue->getOrdinal());
            return;
        } elseif ($enumValue->getName() !== null || $enumValue->getValue() !== null) {
            $type = BinaryTypeStorage::getEntity()->getType($enumValue->getTypeId());
            if ($type && $type->isEnum()) {
                $enumValues = $type->getEnumValues();
                if ($enumValues) {
                    for ($i = 0; $i < count($enumValues); $i++) {
                        if ($enumValue->getName() === $enumValues[$i][0] ||
                            $enumValue->getValue() === $enumValues[$i][1]) {
                            $buffer->writeInteger($i);
                            return;
                        }
                    }
                }
            }
        }
        ArgumentChecker::illegalArgument('Proper ordinal, name or value must be specified for EnumItem');
    }

    private static function writeDecimal(MessageBuffer $buffer, BigDecimal $decimal): void
    {
        $scale = $decimal->getScale();
        $isNegative = $decimal->isNegative();
        $hexValue = $decimal->getUnscaledValue()->abs()->toBase(16);
        $hexValue = ((strlen($hexValue) % 2 !== 0) ? '000' : '00') . $hexValue;
        if ($isNegative) {
            $hexValue[0] = '8';
        }
        $value = '';
        for ($i = 0; $i < strlen($hexValue); $i += 2) {
            $value .= chr(hexdec(substr($hexValue, $i, 2)));
        }
        $buffer->writeInteger($scale);
        $buffer->writeString($value);
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
    
    private static function writeCollection(MessageBuffer $buffer, $collection, CollectionObjectType $collectionType): void
    {
        $buffer->writeInteger($collection instanceof Set ? $collection->count() : count($collection));
        $buffer->writeByte($collectionType->getSubType());
        foreach ($collection as $element) {
            BinaryWriter::writeObject($buffer, $element, $collectionType->getElementType());
        }
    }    

    private static function writeMap(MessageBuffer $buffer, $map, MapObjectType $mapType): void
    {
        if (!($map instanceof Map)) {
            $map = new Map($map);
        }
        $buffer->writeInteger($map->count());
        $buffer->writeByte($mapType->getSubType());
        foreach ($map->pairs() as $pair) {
            BinaryWriter::writeObject($buffer, $pair->key, $mapType->getKeyType());
            BinaryWriter::writeObject($buffer, $pair->value, $mapType->getValueType());
        }
    }

    private static function writeBinaryObject(MessageBuffer $buffer, BinaryObject $binaryObject): void
    {
        $buffer->setPosition($buffer->getPosition() - 1);
        $binaryObject->write($buffer);
    }

    private static function writeComplexObject(MessageBuffer $buffer, object $object, ?ComplexObjectType $objectType): void
    {
        BinaryWriter::writeBinaryObject($buffer, BinaryObject::fromObject($object, $objectType));
    }
}
