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

use Ds\Map;
use Ds\Set;
use Brick\Math\BigDecimal;
use Brick\Math\BigInteger;
use Apache\Ignite\Internal\Connection\ClientFailoverSocket;
use Apache\Ignite\Internal\Utils\ArgumentChecker;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\CollectionObjectType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Data\BinaryObject;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\EnumItem;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;
use Apache\Ignite\Exception\ClientException;

class BinaryCommunicator
{
    private $socket;
    private $typeStorage;
    
    public function __construct(ClientFailoverSocket $socket)
    {
        $this->socket = $socket;
        $this->typeStorage = new BinaryTypeStorage($this);
    }

    public function send(int $opCode, ?callable $payloadWriter, callable $payloadReader = null): void
    {
        $this->socket->send($opCode, $payloadWriter, $payloadReader);
    }
    
    public function getTypeStorage(): BinaryTypeStorage
    {
        return $this->typeStorage;
    }

    public static function readString(MessageBuffer $buffer): ?string
    {
        $typeCode = $buffer->readByte();
        BinaryUtils::checkTypesCompatibility(ObjectType::STRING, $typeCode);
        if ($typeCode === ObjectType::NULL) {
            return null;
        }
        return $buffer->readString();
    }

    public static function writeString(MessageBuffer $buffer, ?string $str): void
    {
        if ($str === null) {
            $buffer->writeByte(ObjectType::NULL);
        } else {
            $buffer->writeByte(ObjectType::STRING);
            $buffer->writeString($str);
        }
    }

    public function readObject(MessageBuffer $buffer, $expectedType = null)
    {
        $typeCode = $buffer->readByte();
        BinaryUtils::checkTypesCompatibility($expectedType, $typeCode);
        return $this->readTypedObject($buffer, $typeCode, $expectedType);
    }

    public function readStringArray(MessageBuffer $buffer): array
    {
        return $this->readTypedObject($buffer, ObjectType::STRING_ARRAY);
    }

    public function writeObject(MessageBuffer $buffer, $object, $objectType = null, bool $writeObjectType = true): void
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
                $this->writeUUID($buffer, $object);
                break;
            case ObjectType::DATE:
                $this->writeDate($buffer, $object);
                break;
            case ObjectType::ENUM:
                $this->writeEnum($buffer, $object);
                break;
            case ObjectType::DECIMAL:
                $this->writeDecimal($buffer, $object);
                break;
            case ObjectType::TIME:
                $this->writeTime($buffer, $object);
                break;
            case ObjectType::TIMESTAMP:
                $this->writeTimestamp($buffer, $object);
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
                $this->writeArray($buffer, $object, $objectType, $objectTypeCode);
                break;
            case ObjectType::COLLECTION:
                $this->writeCollection($buffer, $object, $objectType);
                break;
            case ObjectType::MAP:
                $this->writeMap($buffer, $object, $objectType);
                break;
            case ObjectType::BINARY_OBJECT:
                $this->writeBinaryObject($buffer, $object);
                break;
            case ObjectType::COMPLEX_OBJECT:
                $this->writeComplexObject($buffer, $object, $objectType);
                break;
            default:
                BinaryUtils::unsupportedType($objectType);
        }
    }

    public function readTypedObject(MessageBuffer $buffer, int $objectTypeCode, $expectedType = null)
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
                return $this->readUUID($buffer);
            case ObjectType::DATE:
                return $this->readDate($buffer);
            case ObjectType::ENUM:
            case ObjectType::BINARY_ENUM:
                return $this->readEnum($buffer);
            case ObjectType::DECIMAL:
                return $this->readDecimal($buffer);
            case ObjectType::TIME:
                return $this->readTime($buffer);
            case ObjectType::TIMESTAMP:
                return $this->readTimestamp($buffer);
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
                return $this->readArray($buffer, $objectTypeCode, $expectedType);
            case ObjectType::COLLECTION:
                return $this->readCollection($buffer, $expectedType);
            case ObjectType::MAP:
                return $this->readMap($buffer, $expectedType);
            case ObjectType::BINARY_OBJECT:
                return $this->readBinaryObject($buffer, $expectedType);
            case ObjectType::NULL:
                return null;
            case ObjectType::COMPLEX_OBJECT:
                return $this->readComplexObject($buffer, $expectedType);
            default:
                BinaryUtils::unsupportedType($objectTypeCode);
        }
        return null;
    }

    private function readDate(MessageBuffer $buffer): Date
    {
        return new Date($buffer->readLong());
    }

    private function readTime(MessageBuffer $buffer): Time
    {
        return new Time($buffer->readLong());
    }

    private function readTimestamp(MessageBuffer $buffer): Timestamp
    {
        return new Timestamp($buffer->readLong(), $buffer->readInteger());
    }

    private function readUUID(MessageBuffer $buffer): array
    {
        $result = [];
        for ($i = 0; $i < BinaryUtils::getSize(ObjectType::UUID); $i++) {
            array_push($result, $buffer->readByte(false));
        }
        return $result;
    }

    private function readEnum(MessageBuffer $buffer): EnumItem
    {
        $enumItem = new EnumItem($buffer->readInteger());
        $ordinal = $buffer->readInteger();
        $enumItem->setOrdinal($ordinal);
        $type = $this->typeStorage->getType($enumItem->getTypeId());
        if (!$type || !$type->isEnum()) {
            BinaryUtils::enumSerializationError(false, sprintf('enum type id "%d" is not registered', $enumItem->getTypeId()));
        } elseif (!$type->getEnumValues() || count($type->getEnumValues()) <= $ordinal) {
            BinaryUtils::enumSerializationError(false, 'type mismatch');
        }
        $enumValues = $type->getEnumValues();
        $enumItem->setName($enumValues[$ordinal][0]);
        $enumItem->setValue($enumValues[$ordinal][1]);
        return $enumItem;
    }

    private function readDecimal(MessageBuffer $buffer): BigDecimal
    {
        $scale = $buffer->readInteger();
        $value = $buffer->readString(false);
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
            $result = $result->multipliedBy((BigInteger::of(10))->power(-$scale));
        }
        return $result;
    }

    private function readArray(MessageBuffer $buffer, int $arrayTypeCode, $arrayType): array
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
                    $this->readObject($buffer, $elementType) :
                    $this->readTypedObject($buffer, $elementType));
        }
        return $result;
    }

    private function readCollection(MessageBuffer $buffer, CollectionObjectType $expectedColType = null)
    {
        $size = $buffer->readInteger();
        $subType = $buffer->readByte();
        $isSet = CollectionObjectType::isSet($subType);
        $result = $isSet ? new Set() : [];
        for ($i = 0; $i < $size; $i++) {
            $element = $this->readObject($buffer, $expectedColType ? $expectedColType->getElementType() : null);
            if ($isSet) {
                $result->add($element);
            } else {
                array_push($result, $element);
            }
        }
        return $result;
    }

    private function readMap(MessageBuffer $buffer, MapObjectType $expectedMapType = null): Map
    {
        $size = $buffer->readInteger();
        // map sub-type
        $buffer->readByte();
        $result = new Map();
        $result->allocate($size);
        for ($i = 0; $i < $size; $i++) {
            $key = $this->readObject($buffer, $expectedMapType ? $expectedMapType->getKeyType() : null);
            $value = $this->readObject($buffer, $expectedMapType ? $expectedMapType->getValueType() : null);
            $result->put($key, $value);
        }
        return $result;
    }

    private function readBinaryObject(MessageBuffer $buffer, ?ComplexObjectType $expectedType): object
    {
        $size = $buffer->readInteger();
        $startPos = $buffer->getPosition();
        $buffer->setPosition($startPos + $size);
        $offset = $buffer->readInteger();
        $endPos = $buffer->getPosition();
        $buffer->setPosition($startPos + $offset);
        $result = $this->readObject($buffer, $expectedType);
        $buffer->setPosition($endPos);
        return $result;
    }

    private function readComplexObject(MessageBuffer $buffer, ?ComplexObjectType $expectedType): object
    {
        $buffer->setPosition($buffer->getPosition() - 1);
        $binaryObject = BinaryObject::fromBuffer($this, $buffer);
        return $expectedType ? $binaryObject->toObject($expectedType) : $binaryObject;
    }

    private function writeDate(MessageBuffer $buffer, Date $date): void
    {
        $buffer->writeLong($date->getMillis());
    }

    private function writeTime(MessageBuffer $buffer, Time $time): void
    {
        $buffer->writeLong($time->getMillis());
    }

    private function writeTimestamp(MessageBuffer $buffer, Timestamp $timestamp): void
    {
        $buffer->writeLong($timestamp->getMillis());
        $buffer->writeInteger($timestamp->getNanos());
    }

    private function writeUUID(MessageBuffer $buffer, array $value): void
    {
        for ($i = 0; $i < count($value); $i++) {
            $buffer->writeByte($value[$i], false);
        }
    }

    private function writeEnum(MessageBuffer $buffer, EnumItem $enumValue): void
    {
        $type = $this->typeStorage->getType($enumValue->getTypeId());
        if (!$type || !$type->isEnum()) {
            BinaryUtils::enumSerializationError(true, sprintf('enum type id "%d" is not registered', $enumValue->getTypeId()));
        }
        $buffer->writeInteger($enumValue->getTypeId());
        if ($enumValue->getOrdinal() !== null) {
            $buffer->writeInteger($enumValue->getOrdinal());
            return;
        } elseif ($enumValue->getName() !== null || $enumValue->getValue() !== null) {
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
        ArgumentChecker::illegalArgument('Proper ordinal, name or value must be specified for EnumItem');
    }

    private function writeDecimal(MessageBuffer $buffer, BigDecimal $decimal): void
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
        $buffer->writeString($value, false);
    }

    private function writeArray(MessageBuffer $buffer, array $array, $arrayType, int $arrayTypeCode): void
    {
        $elementType = BinaryUtils::getArrayElementType($arrayType);
        $keepElementType = !$elementType || TypeInfo::getTypeInfo($arrayTypeCode)->keepElementType();
        if ($arrayTypeCode === ObjectType::OBJECT_ARRAY) {
            $typeId = -1;
            if ($elementType instanceof ComplexObjectType) {
                $typeName = BinaryTypeBuilder::calcTypeName($elementType, count($array) > 0 ? $array[0] : null);
                if ($typeName) {
                    $typeId = BinaryType::calculateId($typeName);
                }
            }
            $buffer->writeInteger($typeId);
        }
        $buffer->writeInteger(count($array));
        foreach ($array as $elem) {
            $this->writeObject($buffer, $elem, $elementType, $keepElementType);
        }
    }

    private function writeCollection(MessageBuffer $buffer, $collection, CollectionObjectType $collectionType): void
    {
        $buffer->writeInteger($collection instanceof Set ? $collection->count() : count($collection));
        $buffer->writeByte($collectionType->getSubType());
        foreach ($collection as $element) {
            $this->writeObject($buffer, $element, $collectionType->getElementType());
        }
    }

    private function writeMap(MessageBuffer $buffer, $map, MapObjectType $mapType): void
    {
        if (!($map instanceof Map)) {
            $map = new Map($map);
        }
        $buffer->writeInteger($map->count());
        $buffer->writeByte($mapType->getSubType());
        foreach ($map->pairs() as $pair) {
            $this->writeObject($buffer, $pair->key, $mapType->getKeyType());
            $this->writeObject($buffer, $pair->value, $mapType->getValueType());
        }
    }

    private function writeBinaryObject(MessageBuffer $buffer, BinaryObject $binaryObject): void
    {
        $buffer->setPosition($buffer->getPosition() - 1);
        $binaryObject->write($this, $buffer);
    }

    private function writeComplexObject(MessageBuffer $buffer, object $object, ?ComplexObjectType $objectType): void
    {
        $this->writeBinaryObject($buffer, BinaryObject::fromObject($object, $objectType));
    }
}
