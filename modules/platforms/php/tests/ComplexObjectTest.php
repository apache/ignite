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

namespace Apache\Ignite\Tests;

use Ds\Map;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Data\BinaryObject;

class Class1
{
    public $field_1_1;
    public $field_1_2;
    public $field_1_3;
    
    public function __construct()
    {
        $this->field_1_1 = null;
        $this->field_1_2 = new Class2();
        $this->field_1_3 = null;
    }
}

class SubClass1 extends Class1
{
    public $field_1_4;
    public $field_1_5;
    public $field_1_6;
    public $field_1_7;
    public $field_1_8;
    
    public function __construct()
    {
        parent::__construct();
        $this->field_1_4 = null;
        $this->field_1_5 = new Class3();
        $this->field_1_6 = null;
        $this->field_1_7 = null;
        $this->field_1_8 = null;
    }
}

class Class2
{
    public $field_2_1;
    public $field_2_2;
}

class Class3 {
    public $field_3_1;
    public $field_3_2;
}

final class ComplexObjectTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_compl_obj_cache';
    
    private static $cache;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        self::$cache = TestingHelper::$client->getOrCreateCache(self::CACHE_NAME);
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testPutGetComplexObjects(): void
    {
        $value1 = new Class1();
        $value1->field_1_1 = $this->getPrimitiveValue(ObjectType::BYTE);
        $value1->field_1_2->field_2_1 = $this->getPrimitiveValue(ObjectType::SHORT);
        $value1->field_1_2->field_2_2 = $this->getPrimitiveValue(ObjectType::INTEGER);
        $value1->field_1_3 = $this->getPrimitiveValue(ObjectType::LONG);

        $valueType1 = (new ComplexObjectType())->
            setFieldType('field_1_1', ObjectType::BYTE)->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2ShortInteger')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', ObjectType::SHORT)->
                setFieldType('field_2_2', ObjectType::INTEGER))->
            setFieldType('field_1_3', ObjectType::LONG);

        $value2 = new SubClass1();
        $value2->field_1_1 = $this->getPrimitiveValue(ObjectType::FLOAT);
        $value2->field_1_2->field_2_1 = $this->getPrimitiveValue(ObjectType::DOUBLE);
        $value2->field_1_2->field_2_2 = $this->getPrimitiveValue(ObjectType::CHAR);
        $value2->field_1_3 = $this->getPrimitiveValue(ObjectType::BOOLEAN);
        $value2->field_1_4 = $this->getPrimitiveValue(ObjectType::STRING);
        $value2->field_1_5->field_3_1 = $this->getPrimitiveValue(ObjectType::DATE);
        $value2->field_1_5->field_3_2 = $this->getPrimitiveValue(ObjectType::UUID);
        $value2->field_1_6 = $this->getPrimitiveValue(ObjectType::DECIMAL);
        $value2->field_1_7 = $this->getPrimitiveValue(ObjectType::TIMESTAMP);
        $value2->field_1_8 = $this->getPrimitiveValue(ObjectType::TIME);

        $valueType2 = (new ComplexObjectType())->
            setFieldType('field_1_1', ObjectType::FLOAT)->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2DoubleChar')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', ObjectType::DOUBLE)->
                setFieldType('field_2_2', ObjectType::CHAR))->
            setFieldType('field_1_3', ObjectType::BOOLEAN)->
            setFieldType('field_1_4', ObjectType::STRING)->
            setFieldType('field_1_5', (new ComplexObjectType())->
                setFieldType('field_3_1', ObjectType::DATE)->
                setFieldType('field_3_2', ObjectType::UUID))->
            setFieldType('field_1_6', ObjectType::DECIMAL)->
            setFieldType('field_1_7', ObjectType::TIMESTAMP)->
            setFieldType('field_1_8', ObjectType::TIME);

        $this->putGetComplexObjectsWithDifferentTypes(
            $value1, $value2, $valueType1, $valueType2, Class1::class, SubClass1::class);
    }
    
    public function testPutGetComplexObjectsWithArrays(): void
    {
        $value1 = new Class1();
        $value1->field_1_1 = $this->getArrayValues(ObjectType::BYTE_ARRAY);
        $value1->field_1_2->field_2_1 = $this->getArrayValues(ObjectType::SHORT_ARRAY);
        $value1->field_1_2->field_2_2 = $this->getArrayValues(ObjectType::INTEGER_ARRAY);
        $value1->field_1_3 = $this->getArrayValues(ObjectType::LONG_ARRAY);

        $valueType1 = (new ComplexObjectType())->
            setIgniteTypeName('Class1WithArrays')->
            setPhpClassName(Class1::class)->
            setFieldType('field_1_1', ObjectType::BYTE_ARRAY)->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2WithShortIntegerArrays')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', ObjectType::SHORT_ARRAY)->
                setFieldType('field_2_2', ObjectType::INTEGER_ARRAY))->
            setFieldType('field_1_3', ObjectType::LONG_ARRAY);

        $value2 = new SubClass1();
        $value2->field_1_1 = $this->getArrayValues(ObjectType::FLOAT_ARRAY);
        $value2->field_1_2->field_2_1 = $this->getArrayValues(ObjectType::DOUBLE_ARRAY);
        $value2->field_1_2->field_2_2 = $this->getArrayValues(ObjectType::CHAR_ARRAY);
        $value2->field_1_3 = $this->getArrayValues(ObjectType::BOOLEAN_ARRAY);
        $value2->field_1_4 = $this->getArrayValues(ObjectType::STRING_ARRAY);
        $value2->field_1_5->field_3_1 = $this->getArrayValues(ObjectType::DATE_ARRAY);
        $value2->field_1_5->field_3_2 = $this->getArrayValues(ObjectType::UUID_ARRAY);
        $value2->field_1_6 = $this->getArrayValues(ObjectType::DECIMAL_ARRAY);
        $value2->field_1_7 = $this->getArrayValues(ObjectType::TIMESTAMP_ARRAY);
        $value2->field_1_8 = $this->getArrayValues(ObjectType::TIME_ARRAY);

        $valueType2 = (new ComplexObjectType())->
            setIgniteTypeName('SubClass1WithArrays')->
            setPhpClassName(SubClass1::class)->
            setFieldType('field_1_1', ObjectType::FLOAT_ARRAY)->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2WithDoubleCharArrays')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', ObjectType::DOUBLE_ARRAY)->
                setFieldType('field_2_2', ObjectType::CHAR_ARRAY))->
            setFieldType('field_1_3', ObjectType::BOOLEAN_ARRAY)->
            setFieldType('field_1_4', ObjectType::STRING_ARRAY)->
            setFieldType('field_1_5', (new ComplexObjectType())->
                setIgniteTypeName('Class3WithArrays')->
                setPhpClassName(Class3::class)->
                setFieldType('field_3_1', ObjectType::DATE_ARRAY)->
                setFieldType('field_3_2', ObjectType::UUID_ARRAY))->
            setFieldType('field_1_6', ObjectType::DECIMAL_ARRAY)->
            setFieldType('field_1_7', ObjectType::TIMESTAMP_ARRAY)->
            setFieldType('field_1_8', ObjectType::TIME_ARRAY);

        $this->putGetComplexObjectsWithDifferentTypes(
            $value1, $value2, $valueType1, $valueType2, Class1::class, SubClass1::class, true);
    }

    public function testPutGetComplexObjectsWithMaps(): void
    {
        $value1 = new Class1();
        $value1->field_1_1 = $this->getMapValue(ObjectType::BYTE);
        $value1->field_1_2->field_2_1 = $this->getMapValue(ObjectType::SHORT);
        $value1->field_1_2->field_2_2 = $this->getMapValue(ObjectType::INTEGER);
        $value1->field_1_3 = $this->getMapValue(ObjectType::LONG);

        $valueType1 = (new ComplexObjectType())->
            setIgniteTypeName('Class1WithMaps')->
            setPhpClassName(Class1::class)->
            setFieldType('field_1_1', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::BYTE, ObjectType::BYTE))->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2WithShortIntegerMaps')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::SHORT, ObjectType::SHORT))->
                setFieldType('field_2_2', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::INTEGER, ObjectType::INTEGER)))->
            setFieldType('field_1_3', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::LONG));

        $value2 = new SubClass1();
        $value2->field_1_1 = $this->getMapValue(ObjectType::FLOAT);
        $value2->field_1_2->field_2_1 = $this->getMapValue(ObjectType::DOUBLE);
        $value2->field_1_2->field_2_2 = $this->getMapValue(ObjectType::CHAR);
        $value2->field_1_3 = $this->getMapValue(ObjectType::BOOLEAN);
        $value2->field_1_4 = $this->getMapValue(ObjectType::STRING);
        $value2->field_1_5->field_3_1 = $this->getMapValue(ObjectType::DATE);
        $value2->field_1_5->field_3_2 = $this->getMapValue(ObjectType::UUID);
        $value2->field_1_6 = $this->getMapValue(ObjectType::DECIMAL);
        $value2->field_1_7 = $this->getMapValue(ObjectType::TIMESTAMP);
        $value2->field_1_8 = $this->getMapValue(ObjectType::TIME);

        $valueType2 = (new ComplexObjectType())->
            setIgniteTypeName('SubClass1WithArrays')->
            setPhpClassName(SubClass1::class)->
            setFieldType('field_1_1', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::FLOAT))->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2WithDoubleCharArrays')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::DOUBLE))->
                setFieldType('field_2_2', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::CHAR, ObjectType::CHAR)))->
            setFieldType('field_1_3', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::BOOLEAN, ObjectType::BOOLEAN))->
            setFieldType('field_1_4', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::STRING))->
            setFieldType('field_1_5', (new ComplexObjectType())->
                setIgniteTypeName('Class3WithArrays')->
                setPhpClassName(Class3::class)->
                setFieldType('field_3_1', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::DATE))->
                setFieldType('field_3_2', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::UUID)))->
            setFieldType('field_1_6', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::DECIMAL))->
            setFieldType('field_1_7', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::TIMESTAMP))->
            setFieldType('field_1_8', new MapObjectType(MapObjectType::HASH_MAP, ObjectType::STRING, ObjectType::TIME));

        $this->putGetComplexObjectsWithMaps(
            $value1, $value2, $valueType1, $valueType2, Class1::class, SubClass1::class);
    }

    public function testPutGetBinaryObjectsFromObjects(): void
    {
        $valueType = (new ComplexObjectType())->
            setIgniteTypeName('Class1WithStringObjStringArray')->
            setPhpClassName(Class1::class)->
            setFieldType('field_1_1', ObjectType::STRING)->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2WithShortBoolean')->
                setPhpClassName(Class2::class)->
                setFieldType('field_2_1', ObjectType::SHORT)->
                setFieldType('field_2_2', ObjectType::BOOLEAN))->
            setFieldType('field_1_3', ObjectType::STRING_ARRAY);
        $this->putGetBinaryObjects($valueType);
        $defaultValueType = (new ComplexObjectType())->
            setIgniteTypeName('Class1Default')->
            setPhpClassName(Class1::class)->
            setFieldType('field_1_2', (new ComplexObjectType())->
                setIgniteTypeName('Class2Default')->
                setPhpClassName(Class2::class));
        $this->putGetBinaryObjects($defaultValueType);
    }
    
    private function putGetComplexObjectsWithDifferentTypes(
        $key, $value, $keyType, $valueType, $keyClass, $valueClass, $isNullable = false)
    {
        $this->putGetComplexObjects($key, $value, $keyType, $valueType, $value);

        $binaryKey = BinaryObject::fromObject($key, $keyType);
        $binaryValue = BinaryObject::fromObject($value, $valueType);
        $this->putGetComplexObjects($binaryKey, $binaryValue, null, null, $value);

        if ($isNullable) {
            $this->putGetComplexObjects(new $keyClass(), new $valueClass(), $keyType, $valueType, new $valueClass());
        }

        if ($isNullable) {
            $binaryKey = BinaryObject::fromObject(new $keyClass(), $keyType);
            $binaryValue = BinaryObject::fromObject(new $valueClass(), $valueType);
            $this->putGetComplexObjects($binaryKey, $binaryValue, null, null, new $valueClass());
        }
    }

    private function putGetComplexObjectsWithMaps(
        $key, $value, $keyType, $valueType, $keyClass, $valueClass)
    {
        $this->putGetComplexObjects($key, $value, $keyType, $valueType, $value);

        $this->putGetComplexObjects(new $keyClass(), new $valueClass(), $keyType, $valueType, new $valueClass());
    }

    private function putGetComplexObjects($key, $value, $keyType, $valueType, $valuePattern)
    {
        self::$cache->
            setKeyType($keyType)->
            setValueType($valueType);
        try {
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            $strResult = TestingHelper::printValue($result);
            $strValue = TestingHelper::printValue($valuePattern);
            $this->assertTrue(
                TestingHelper::compare($valuePattern, $result),
                "values are not equal: put value={$strValue}, get value={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }
    
    private function putGetBinaryObjects($valueType): void
    {
        $value1 = new Class1();
        $value1->field_1_1 = 'abc';
        $value1->field_1_2->field_2_1 = 1234;
        $value1->field_1_2->field_2_2 = true;
        $value1->field_1_3 = ['a', 'bb', 'ccc'];

        $value2 = new Class1();
        $value2->field_1_1 = 'def';
        $value2->field_1_2->field_2_1 = 5432;
        $value2->field_1_2->field_2_2 = false;
        $value2->field_1_3 = ['a', 'bb', 'ccc', 'dddd'];

        $value3 = new Class1();
        $value3->field_1_1 = 'defdef';
        $value3->field_1_2->field_2_1 = 543;
        $value3->field_1_2->field_2_2 = false;
        $value3->field_1_3 = ['a', 'bb', 'ccc', 'dddd', 'eeeee'];

        $field_1_2_Type = $valueType ? $valueType->getFieldType('field_1_2') : null;

        $binaryValue1 = BinaryObject::fromObject($value1, $valueType);
        $binaryValue2 = BinaryObject::fromObject($value2, $valueType);
        $binaryValue3 = BinaryObject::fromObject($value3, $valueType);

        self::$cache->
            setKeyType(null)->
            setValueType(null);
        $cache = self::$cache;
        try {
            $cache->put($binaryValue1, $binaryValue2);
            $result = $cache->get($binaryValue1);
            $this->binaryObjectEquals($result, $value2, $valueType);

            $binaryValue1->setField('field_1_1', 'abcde');
            $result = $cache->get($binaryValue1);
            $this->assertTrue($result === null);

            $binaryValue2->setField('field_1_1', $value3->field_1_1);
            $binaryValue2->setField('field_1_2', $value3->field_1_2, $field_1_2_Type);
            $binaryValue2->setField('field_1_3', $value3->field_1_3);
            $cache->put($binaryValue1, $binaryValue2);
            $result = $cache->get($binaryValue1);
            $this->binaryObjectEquals($result, $value3, $valueType);

            $binaryValue1->setField('field_1_1', 'abc');
            $binaryValue1->setField('field_1_3', $binaryValue1->getField('field_1_3'));
            $result = $cache->get($binaryValue1);
            $this->binaryObjectEquals($result, $value2, $valueType);

            $result = $cache->get($binaryValue1);
            $this->binaryObjectEquals($result, $value2, $valueType);

            $binaryValue3->setField('field_1_1', $result->getField('field_1_1'));
            $binaryValue3->setField('field_1_2', $result->getField('field_1_2', $field_1_2_Type), $field_1_2_Type);
            $binaryValue3->setField('field_1_3', $result->getField('field_1_3'));
            $cache->put($binaryValue1, $binaryValue3);
            $result = $cache->get($binaryValue1);
            $this->binaryObjectEquals($result, $value2, $valueType);
        } finally {
            self::$cache->removeAll();
        }
    }
    
    private function binaryObjectEquals(BinaryObject $binaryObj, $valuePattern, $valueType): void
    {
        $strBinObj = TestingHelper::printValue($binaryObj);
        $strValue = TestingHelper::printValue($valuePattern);
        $this->assertTrue(
            TestingHelper::compare($valuePattern, $binaryObj),
            "binary values are not equal: put value={$strValue}, get value={$strBinObj}");
        if ($valueType) {
            $object = $binaryObj->toObject($valueType);
            $strObject = TestingHelper::printValue($object);
            $this->assertTrue(
                TestingHelper::compare($valuePattern, $object),
                "values are not equal: put value={$strValue}, get value={$strObject}");
        }
    }
    
    private function getPrimitiveValue(int $typeCode)
    {
        return TestingHelper::$primitiveValues[$typeCode]['values'][0];
    }
    
    private function getArrayValues(int $typeCode)
    {
        return TestingHelper::$primitiveValues[TestingHelper::$arrayValues[$typeCode]['elemType']]['values'];
    }
    
    private function getMapValue(int $typeCode)
    {
        $map = new Map();
        $values = TestingHelper::$primitiveValues[$typeCode]['values'];
        $length = count($values);
        for ($i = 0; $i < $length; $i++) {
            $value = $values[$i];
            if (!TestingHelper::$primitiveValues[$typeCode]['isMapKey']) {
                $value = print_r($value, true);
            }
            $map->put($value, $values[$length - $i - 1]);
        }
        return $map;
    }
    
    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(self::CACHE_NAME);
    }
}
