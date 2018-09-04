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
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Type\CollectionObjectType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Type\ObjectArrayType;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;
use Apache\Ignite\Data\EnumItem;
use Apache\Ignite\Data\BinaryObject;
use Apache\Ignite\Internal\Utils\ArgumentChecker;

class BinaryUtils
{
    const FLOAT_EPSILON = 0.00001;
    public static $is32BitInt = PHP_INT_MAX === TypeInfo::MAX_INT_VALUE;

    public static function getSize(int $typeCode): int
    {
        return TypeInfo::getTypeInfo($typeCode)->getSize();
    }

    public static function checkCompatibility($value, $type): void
    {
        if (!$type) {
            return;
        }
        $typeCode = BinaryUtils::getTypeCode($type);
        if ($value === null) {
            if (!TypeInfo::getTypeInfo($typeCode)->isNullable()) {
                BinaryUtils::typeCastError(ObjectType::NULL, $typeCode);
            }
            return;
        } elseif (BinaryUtils::isStandardType($typeCode)) {
            BinaryUtils::checkStandardTypeCompatibility($value, $typeCode, $type);
            return;
        }
        $valueTypeCode = BinaryUtils::getTypeCode(BinaryUtils::calcObjectType($value));
        if ($typeCode !== $valueTypeCode) {
            BinaryUtils::typeCastError($valueTypeCode, $typeCode);
        }
    }

    public static function isStandardType($typeCode): bool
    {
        return $typeCode !== ObjectType::BINARY_OBJECT &&
            $typeCode !== ObjectType::COMPLEX_OBJECT;
    }

    public static function checkStandardTypeCompatibility($value, $typeCode, $type = null, $signed = true)
    {
        $valueType = BinaryUtils::getPhpTypeName($value);
        switch ($typeCode) {
            case ObjectType::BYTE:
            case ObjectType::SHORT:
            case ObjectType::INTEGER:
                if (!is_integer($value)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                $typeInfo = TypeInfo::getTypeInfo($typeCode);
                $min = $typeInfo->getMinValue();
                $max = $typeInfo->getMaxValue();
                if ($signed && ($min && $value < $min || $max && $value > $max) ||
                    !$signed && ($value < 0 || $value > $max - $min)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::LONG:
            case ObjectType::FLOAT:
            case ObjectType::DOUBLE:
                if (!is_integer($value) && !is_float($value)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::CHAR:
                if (!is_string($value) || mb_strlen($value) !== 1) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::BOOLEAN:
                if (!is_bool($value)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::STRING:
                if (!is_string($value)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::UUID:
                if (!BinaryUtils::isIndexedArray($value) ||
                    count($value) !== BinaryUtils::getSize(ObjectType::UUID)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                foreach ($value as $element) {
                    BinaryUtils::checkStandardTypeCompatibility($element, ObjectType::BYTE, null, false);
                }
                return;
            case ObjectType::DATE:
                if (!($value instanceof Date)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::ENUM:
                if (!($value instanceof EnumItem)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::DECIMAL:
                if (!($value instanceof BigDecimal)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::TIMESTAMP:
                if (!($value instanceof Timestamp)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
            case ObjectType::TIME:
                if (!($value instanceof Time)) {
                    BinaryUtils::valueCastError($value, $typeCode);
                }
                return;
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
                if (!BinaryUtils::isIndexedArray($value)) {
                    BinaryUtils::typeCastError($valueType, $typeCode);
                }
                return;
            case ObjectType::MAP:
                if (!($value instanceof Map) && !is_array($value)) {
                    BinaryUtils::typeCastError($valueType, $typeCode);
                }
                return;
            case ObjectType::COLLECTION:
                $isSet = $type && CollectionObjectType::isSet($type->getSubType());
                if (!($isSet && $value instanceof Set || BinaryUtils::isIndexedArray($value))) {
                    BinaryUtils::typeCastError($valueType, $isSet ? 'set' : $typeCode);
                }
                return;
            case ObjectType::NULL:
                if ($value !== null) {
                    BinaryUtils::typeCastError('not null', $typeCode);
                }
                return;
            default:
                $valueTypeCode = BinaryUtils::getTypeCode(BinaryUtils::calcObjectType($value));
                if ($valueTypeCode === ObjectType::BINARY_OBJECT) {
                    BinaryUtils::typeCastError($valueTypeCode, $typeCode);
                }
                return;
        }
    }

    public static function checkTypesCompatibility($expectedType, int $actualTypeCode): void
    {
        if ($expectedType === null) {
            return;
        }
        $expectedTypeCode = BinaryUtils::getTypeCode($expectedType);
        if ($actualTypeCode === ObjectType::NULL) {
            return;
        } elseif ($expectedTypeCode === ObjectType::BINARY_OBJECT ||
            $actualTypeCode === ObjectType::BINARY_OBJECT &&
            $expectedTypeCode === ObjectType::COMPLEX_OBJECT) {
            return;
        } elseif ($actualTypeCode !== $expectedTypeCode) {
            BinaryUtils::typeCastError($actualTypeCode, $expectedTypeCode);
        }
    }

    public static function calcObjectType($object)
    {
        if (is_integer($object)) {
            return ObjectType::INTEGER;
        } elseif (is_float($object)) {
            return ObjectType::DOUBLE;
        } elseif (is_string($object)) {
            return ObjectType::STRING;
        } elseif (is_bool($object)) {
            return ObjectType::BOOLEAN;
        } elseif (is_array($object)) {
            if (count($object) > 0) {
                if (BinaryUtils::isIndexedArray($object)) {
                    if ($object[0] !== null) {
                        // indexed array
                        return BinaryUtils::getArrayType(BinaryUtils::calcObjectType($object[0]));
                    }
                } else {
                    // associative array
                    return new MapObjectType();
                }
            } else {
                BinaryUtils::noDefaultMapping("empty array");
            }
        } elseif ($object instanceof Time) {
            return ObjectType::TIME;
        } elseif ($object instanceof Timestamp) {
            return ObjectType::TIMESTAMP;
        } elseif ($object instanceof Date) {
            return ObjectType::DATE;
        } elseif ($object instanceof EnumItem) {
            return ObjectType::ENUM;
        } elseif ($object instanceof BigDecimal) {
            return ObjectType::DECIMAL;
        } elseif ($object instanceof Set) {
            return new CollectionObjectType(CollectionObjectType::HASH_SET);
        } elseif ($object instanceof Map) {
            return new MapObjectType();
        } elseif ($object instanceof BinaryObject) {
            return ObjectType::BINARY_OBJECT;
        } elseif (is_object($object)) {
            return new ComplexObjectType();
        }
        BinaryUtils::noDefaultMapping(BinaryUtils::getPhpTypeName($object));
        return null;
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
            case ObjectType::BINARY_OBJECT:
                return new ObjectArrayType();
            default:
                return new ObjectArrayType($elementType);
        }
    }

    public static function getArrayElementType($arrayType)
    {
        if ($arrayType instanceof ObjectArrayType) {
            return $arrayType->getElementType();
        } elseif ($arrayType === ObjectType::OBJECT_ARRAY) {
            return null;
        }
        $info = TypeInfo::getTypeInfo($arrayType);
        if (!$info || !$info->getElementTypeCode()) {
            BinaryUtils::internalError();
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

    public static function getPhpTypeName($object): string
    {
        if (is_array($object) && !BinaryUtils::isIndexedArray($object)) {
            return 'associative array';
        }
        return gettype($object);
    }

    public static function checkObjectType($type, string $argName): void
    {
        if ($type === null || $type instanceof ObjectType) {
            return;
        }
        ArgumentChecker::hasValueFrom($type, $argName, false, TypeInfo::getPrimitiveTypes());
    }
    
    public static function floatEquals(float $val1, float $val2): bool
    {
        return abs($val1 - $val2) < BinaryUtils::FLOAT_EPSILON;
    }

    public static function getLongHex(BigInteger $value, bool $isNegative): string
    {
        $size = TypeInfo::getTypeInfo(ObjectType::LONG)->getSize();
        if ($isNegative) {
            $value = BigInteger::parse(str_pad('1', $size * 2 + 1, '0'), 16)->minus($value);
        }
        return str_pad($value->toBase(16), $size * 2, '0', STR_PAD_LEFT);
    }

    public static function hashCode(?string $str): int
    {
        $hash = 0;
        $length = strlen($str);
        if ($str && $length > 0) {
            for ($i = 0; $i < $length; $i++) {
                $hash = (($hash << 5) - $hash) + ord($str[$i]);
                $hash &= 0xFFFFFFFF; // Convert to 32bit integer
            }
        }
        return BinaryUtils::intVal32($hash);
    }
    
    public static function hashCodeLowerCase(?string $str): int
    {
        return BinaryUtils::hashCode($str ? strtolower($str) : $str);
    }

    public static function contentHashCode(MessageBuffer $buffer, int $startPos, int $endPos): int
    {
        $hash = 1;
        $length = $endPos - $startPos + 1;
        $content = $buffer->getSlice($startPos, $length);
        for ($i = 0; $i < $length; $i++) {
            $hash = 31 * $hash + ord($content[$i]);
            $hash &= 0xFFFFFFFF; // Convert to 32bit integer
        }
        return BinaryUtils::intVal32($hash);
    }
    
    public static function intVal32(int $value): int
    {
        if (!BinaryUtils::$is32BitInt) {
            $value = ($value & 0xFFFFFFFF);
            if ($value & 0x80000000) {
                $value = -((~$value & 0xFFFFFFFF) + 1);
            }
        }
        return $value;
    }

    public static function internalError(string $message = null): void
    {
        throw new ClientException($message ? $message : 'Internal library error');
    }

    public static function unsupportedType($type): void
    {
        throw new ClientException(sprintf('Type %s is not supported', BinaryUtils::getTypeName($type)));
    }

    public static function noDefaultMapping($type): void
    {
        throw new ClientException(sprintf('%s has no default type mapping', BinaryUtils::getTypeName($type)));
    }

    public static function serializationError(bool $serialize, string $message = null): void
    {
        $msg = $serialize ? 'Complex object can not be serialized' : 'Complex object can not be deserialized';
        if ($message) {
            $msg = $msg . ': ' . $message;
        }
        throw new ClientException($msg);
    }

    public static function typeCastError($fromType, $toType): void
    {
        throw new ClientException(sprintf('Type "%s" can not be cast to %s',
            BinaryUtils::getTypeName($fromType), BinaryUtils::getTypeName($toType)));
    }

    public static function valueCastError($value, $toType): void
    {
        throw new ClientException(sprintf('Value "%s" can not be cast to %s',
            print_r($value, true), BinaryUtils::getTypeName($toType)));
    }

    private static function isIndexedArray(array $object): bool
    {
        return $object === array_values($object);
    }
}
