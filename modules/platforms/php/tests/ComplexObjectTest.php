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
use Apache\Ignite\CacheInterface;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Type\ObjectType;
//use Apache\Ignite\Type\MapObjectType;
//use Apache\Ignite\Type\CollectionObjectType;
//use Apache\Ignite\Type\ObjectArrayType;
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
    const CACHE_NAME = '__php_test_cache';
    
    private static $cache;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        ComplexObjectTestCase::cleanUp();
        ComplexObjectTestCase::$cache = TestingHelper::$client->getOrCreateCache(ComplexObjectTestCase::CACHE_NAME);
    }

    public static function tearDownAfterClass(): void
    {
        ComplexObjectTestCase::cleanUp();
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
    
    private function putGetComplexObjectsWithDifferentTypes(
        $key, $value, $keyType, $valueType, $keyClass, $valueClass, $isNullable = false)
    {
        $this->putGetComplexObjects($key, $value, $keyType, $valueType, $value);

        if ($isNullable) {
            $this->putGetComplexObjects(new $keyClass(), new $valueClass(), $keyType, $valueType, new $valueClass());
        }

        $binaryKey = BinaryObject::fromObject($key, $keyType);
        $binaryValue = BinaryObject::fromObject($value, $valueType);
        $this->putGetComplexObjects($binaryKey, $binaryValue, null, null, $value);

        if ($isNullable) {
            $binaryKey = BinaryObject::fromObject(new $keyClass(), $keyType);
            $binaryValue = BinaryObject::fromObject(new $valueClass(), $valueType);
            $this->putGetComplexObjects($binaryKey, $binaryValue, null, null, new $valueClass());
        }
    }
    
    private function putGetComplexObjects($key, $value, $keyType, $valueType, $valuePattern)
    {
        ComplexObjectTestCase::$cache->
            setKeyType($keyType)->
            setValueType($valueType);
        try {
            ComplexObjectTestCase::$cache->put($key, $value);
            $result = ComplexObjectTestCase::$cache->get($key);
            $strResult = TestingHelper::printValue($result);
            $strValue = TestingHelper::printValue($valuePattern);
            $this->assertTrue(
                TestingHelper::compare($valuePattern, $result),
                "values are not equal: put value={$strValue}, get value={$strResult}");
        } finally {
            ComplexObjectTestCase::$cache->removeAll();
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
    
    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(ComplexObjectTestCase::CACHE_NAME);
    }
}
