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

use \DateTime;
use Ds\Map;
use Ds\Set;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Type\CollectionObjectType;
use Apache\Ignite\Type\ObjectArrayType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Data\BinaryObject;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Timestamp;
use Apache\Ignite\Data\EnumItem;
use Apache\Ignite\Exception\ClientException;

class TstComplObjectWithPrimitiveFields
{
    public $field1;
    public $field2;
    public $field3;
    public $field4;
    public $field5;
    public $field6;
    public $field7;
    public $field8;
    public $field9;
    public $field10;
    public $field11;
    public $field30;
    public $field33;
    public $field36;
}

class TstComplObjectWithDefaultFieldTypes
{
    public $field3;
    public $field6;
    public $field8;
    public $field9;
    public $field11;
    public $field30;
    public $field33;
    public $field36;
}

final class CachePutGetTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';

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

    public function testPutGetPrimitiveValues(): void
    {
        foreach (TestingHelper::$primitiveValues as $typeCode1 => $typeInfo1) {
            foreach (TestingHelper::$primitiveValues as $typeCode2 => $typeInfo2) {
                foreach ($typeInfo1['values'] as $value1) {
                    foreach ($typeInfo2['values'] as $value2) {
                        $this->putGetPrimitiveValues($typeCode1, $typeCode2, $value1, $value2);
                        if (array_key_exists('typeOptional', $typeInfo1)) {
                            $this->putGetPrimitiveValues(null, $typeCode2, $value1, $value2);
                        }
                        if (array_key_exists('typeOptional', $typeInfo2)) {
                            $this->putGetPrimitiveValues($typeCode1, null, $value1, $value2);
                        }
                    }
                }
            }
        }
    }

    public function testPutGetArraysOfPrimitives(): void
    {
        foreach (TestingHelper::$arrayValues as $type => $typeInfo) {
            $primitiveType = $typeInfo['elemType'];
            $values = TestingHelper::$primitiveValues[$primitiveType]['values'];
            $this->putGetArrays($primitiveType, $type, $values[0], $values);
            $this->putGetArrays($primitiveType, $type, $values[0], []);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetArrays($primitiveType, $type, $values[0], $values);
            }
        }
    }

    public function testPutGetMaps(): void
    {
        foreach (TestingHelper::$primitiveValues as $type1 => $typeInfo1) {
            if (!$typeInfo1['isMapKey']) {
                continue;
            }
            foreach (TestingHelper::$primitiveValues as $type2 => $typeInfo2) {
                $map = new Map();
                $index2 = 0;
                foreach ($typeInfo1['values'] as $value1) {
                    $value2 = $typeInfo2['values'][$index2];
                    $index2++;
                    if ($index2 >= count($typeInfo2['values'])) {
                        $index2 = 0;
                    }
                    $map->put($value1, $value2);
                }
                $this->putGetMaps(new MapObjectType(MapObjectType::HASH_MAP, $type1, $type2), $map);
                $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1, $type2), $map);
                if (array_key_exists('typeOptional', $typeInfo1)) {
                    $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, null, $type2), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo1) && array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP), $map);
                    $this->putGetMaps(null, $map);
                }
            }
        }
    }

    public function testPutGetArrayMaps(): void
    {
        foreach (TestingHelper::$primitiveValues as $type1 => $typeInfo1) {
            if (!$typeInfo1['isArrayKey']) {
                continue;
            }
            foreach (TestingHelper::$primitiveValues as $type2 => $typeInfo2) {
                $map = [];
                $index2 = 0;
                foreach ($typeInfo1['values'] as $value1) {
                    $value2 = $typeInfo2['values'][$index2];
                    $index2++;
                    if ($index2 >= count($typeInfo2['values'])) {
                        $index2 = 0;
                    }
                    $map[$value1] = $value2;
                }
                $this->putGetArrayMaps(new MapObjectType(MapObjectType::HASH_MAP, $type1, $type2), $map);
                $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1, $type2), $map);
                if (array_key_exists('typeOptional', $typeInfo1)) {
                    $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, null, $type2), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo1) && array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP), $map);
                    $this->putGetArrayMaps(null, $map);
                }
            }
        }
    }

    public function testPutGetMapsWithArrays(): void
    {
        foreach (TestingHelper::$primitiveValues as $type1 => $typeInfo1) {
            if (!$typeInfo1['isMapKey']) {
                continue;
            }
            foreach (TestingHelper::$arrayValues as $type2 => $typeInfo2) {
                $primitiveType2 = $typeInfo2['elemType'];
                $values2 = TestingHelper::$primitiveValues[$primitiveType2]['values'];
                $map = new Map();
                $index2 = 0;
                $arrayValues2 = [$values2, null, array_reverse($values2)];
                foreach ($typeInfo1['values'] as $value1) {
                    $map->put($value1, $arrayValues2[$index2]);
                    $index2++;
                    if ($index2 >= count($arrayValues2)) {
                        $index2 = 0;
                    }
                }
                $this->putGetMaps(new MapObjectType(MapObjectType::HASH_MAP, $type1, $type2), $map);
                $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1, $type2), $map);
                if (array_key_exists('typeOptional', $typeInfo1)) {
                    $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, null, $type2), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo1) && array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP), $map);
                    $this->putGetMaps(null, $map);
                }
            }
        }
    }

    public function testPutGetArrayMapsWithArrays(): void
    {
        foreach (TestingHelper::$primitiveValues as $type1 => $typeInfo1) {
            if (!$typeInfo1['isArrayKey']) {
                continue;
            }
            foreach (TestingHelper::$arrayValues as $type2 => $typeInfo2) {
                $primitiveType2 = $typeInfo2['elemType'];
                $values2 = TestingHelper::$primitiveValues[$primitiveType2]['values'];
                $map = [];
                $index2 = 0;
                $arrayValues2 = [$values2, null, array_reverse($values2)];
                foreach ($typeInfo1['values'] as $value1) {
                    $map[$value1] = $arrayValues2[$index2];
                    $index2++;
                    if ($index2 >= count($arrayValues2)) {
                        $index2 = 0;
                    }
                }
                $this->putGetArrayMaps(new MapObjectType(MapObjectType::HASH_MAP, $type1, $type2), $map);
                $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1, $type2), $map);
                if (array_key_exists('typeOptional', $typeInfo1)) {
                    $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, null, $type2), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1), $map);
                }
                if (array_key_exists('typeOptional', $typeInfo1) && array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetArrayMaps(new MapObjectType(MapObjectType::LINKED_HASH_MAP), $map);
                    $this->putGetArrayMaps(null, $map);
                }
            }
        }
    }

    public function testPutGetSets(): void
    {
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $set = new Set();
            foreach ($typeInfo['values'] as $value) {
                $set->add($value);
            }
            $this->putGetSets(new CollectionObjectType(CollectionObjectType::USER_SET, $type), $set);
            $this->putGetSets(new CollectionObjectType(CollectionObjectType::HASH_SET, $type), $set);
            $this->putGetSets(new CollectionObjectType(CollectionObjectType::LINKED_HASH_SET, $type), $set);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetSets(new CollectionObjectType(CollectionObjectType::LINKED_HASH_SET), $set);
                $this->putGetSets(null, $set);
            }
        }
    }

    public function testPutGetLists(): void
    {
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $list = array();
            foreach ($typeInfo['values'] as $value) {
                array_push($list, $value);
            }
            $this->putGetLists(new CollectionObjectType(CollectionObjectType::USER_COL, $type), $list);
            $this->putGetLists(new CollectionObjectType(CollectionObjectType::ARRAY_LIST, $type), $list);
            $this->putGetLists(new CollectionObjectType(CollectionObjectType::LINKED_LIST, $type), $list);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetLists(new CollectionObjectType(CollectionObjectType::ARRAY_LIST), $list);
            }
//            $singletonList = [$typeInfo['values'][0]];
//            $this->putGetLists(new CollectionObjectType(CollectionObjectType::SINGLETON_LIST, $type), $singletonList);
        }
    }

    public function testPutGetObjectArrayOfMaps(): void
    {
        foreach (TestingHelper::$primitiveValues as $type1 => $typeInfo1) {
            if (!$typeInfo1['isMapKey']) {
                continue;
            }
            foreach (TestingHelper::$primitiveValues as $type2 => $typeInfo2) {
                $map = new Map();
                $index2 = 0;
                foreach ($typeInfo1['values'] as $value1) {
                    $value2 = $typeInfo2['values'][$index2];
                    $index2++;
                    if ($index2 >= count($typeInfo2['values'])) {
                        $index2 = 0;
                    }
                    $map->put($value1, $value2);
                }
                $array = array();
                for ($i = 0; $i < 5; $i++) {
                    $map->reverse();
                    array_push($array, $map);
                }
                $this->putGetObjectArrays(new ObjectArrayType(new MapObjectType(MapObjectType::HASH_MAP, $type1, $type2)), $array);
                if (array_key_exists('typeOptional', $typeInfo1)) {
                    $this->putGetObjectArrays(new ObjectArrayType(new MapObjectType(MapObjectType::LINKED_HASH_MAP, null, $type2)), $array);
                }
                if (array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetObjectArrays(new ObjectArrayType(new MapObjectType(MapObjectType::LINKED_HASH_MAP, $type1)), $array);
                }
                if (array_key_exists('typeOptional', $typeInfo1) && array_key_exists('typeOptional', $typeInfo2)) {
                    $this->putGetObjectArrays(new ObjectArrayType(), $array);
                    $this->putGetObjectArrays(null, $array);
                }
            }
        }
    }

    public function testPutGetObjectArrayOfPrimitives(): void
    {
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $array = $typeInfo['values'];
            $this->putGetObjectArrays(new ObjectArrayType($type), $array);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetObjectArrays(new ObjectArrayType(), $array);
            }
        }
    }

    public function testPutGetObjectArrayOfPrimitiveArrays(): void
    {
        foreach (TestingHelper::$arrayValues as $type => $typeInfo) {
            $primitiveType = $typeInfo['elemType'];
            $values = TestingHelper::$primitiveValues[$primitiveType]['values'];
            $array = [];
            for ($i = 0; $i < 5; $i++) {
                $values = array_reverse($values);
                array_push($array, $values);
            }
            $this->putGetObjectArrays(new ObjectArrayType($type), $array);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetObjectArrays(new ObjectArrayType(), $array);
                $this->putGetObjectArrays(null, $array);
            }
        }
    }

    public function testPutGetObjectArrayOfSets(): void
    {
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $set = new Set();
            $values = $typeInfo['values'];
            $array = array();
            for ($i = 0; $i < 5; $i++) {
                $values = array_reverse($values);
                array_push($array, new Set($values));
            }
            $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::USER_SET, $type)), $array);
            $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::HASH_SET, $type)), $array);
            $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::LINKED_HASH_SET, $type)), $array);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetObjectArrays(new ObjectArrayType(), $array);
                $this->putGetObjectArrays(null, $array);
            }
        }
    }

    public function testPutGetObjectArrayOfLists(): void
    {
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $set = new Set();
            $values = $typeInfo['values'];
            $array = array();
            for ($i = 0; $i < 5; $i++) {
                $values = array_reverse($values);
                array_push($array, $values);
            }
            $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::USER_COL, $type)), $array);
            $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::ARRAY_LIST, $type)), $array);
            $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::LINKED_LIST, $type)), $array);
            if (array_key_exists('typeOptional', $typeInfo)) {
                $this->putGetObjectArrays(new ObjectArrayType(new CollectionObjectType(CollectionObjectType::ARRAY_LIST)), $array);
            }
        }
    }

    public function testPutGetObjectArrayOfComplexObjects(): void
    {
        $array = array();
        for ($i = 0; $i < 5; $i++) {
            $object = new TstComplObjectWithPrimitiveFields();
            foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
                $fieldName = 'field' . $type;
                $index = ($i < count($typeInfo['values'])) ? $i : $i % count($typeInfo['values']);
                $object->$fieldName = $typeInfo['values'][$index];
            }
            array_push($array, $object);
        }

        $fullComplexObjectType = new ComplexObjectType();
        $partComplexObjectType = new ComplexObjectType();
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $fullComplexObjectType->setFieldType('field' . $type, $type);
            if (!array_key_exists('typeOptional', $typeInfo)) {
                $partComplexObjectType->setFieldType('field' . $type, $type);
            }
        }
        $this->putGetObjectArrays(new ObjectArrayType($fullComplexObjectType), $array);
        $this->putGetObjectArrays(new ObjectArrayType($partComplexObjectType), $array);
    }

    public function testPutGetObjectArrayOfComplexObjectsWithDefaultFieldTypes(): void
    {
        $array = array();
        for ($i = 0; $i < 5; $i++) {
            $object = new TstComplObjectWithDefaultFieldTypes();
            foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
                if (!array_key_exists('typeOptional', $typeInfo)) {
                    continue;
                }
                $fieldName = 'field' . $type;
                $index = ($i < count($typeInfo['values'])) ? $i : $i % count($typeInfo['values']);
                $object->$fieldName = $typeInfo['values'][$index];
            }
            array_push($array, $object);
        }
        $this->putGetObjectArrays(new ObjectArrayType(new ComplexObjectType()), $array);
    }

    public function testPutGetObjectArrayOfBinaryObjects(): void
    {
        $array = array();
        for ($i = 0; $i < 5; $i++) {
            $binaryObject = new BinaryObject('tstBinaryObj');
            foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
                $fieldName = 'field' . $type;
                $index = ($i < count($typeInfo['values'])) ? $i : $i % count($typeInfo['values']);
                $binaryObject->setField($fieldName, $typeInfo['values'][$index], $type);
            }
            array_push($array, $binaryObject);
        }
        $this->putGetObjectArrays(new ObjectArrayType(), $array);
        $this->putGetObjectArrays(null, $array);
    }

    public function testPutGetObjectArrayOfObjectArrays(): void
    {
        $complexObjectType = new ComplexObjectType();
        foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
            $complexObjectType->setFieldType('field' . $type, $type);
        }
        $array = array();
        for ($i = 0; $i < 2; $i++) {
            $innerArray = array();
            for ($j = 0; $j < 2; $j++) {
                $object = new TstComplObjectWithPrimitiveFields();
                foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
                    $fieldName = 'field' . $type;
                    $index = $i * 10 + $j;
                    $index = ($index < count($typeInfo['values'])) ? $index : $index % count($typeInfo['values']);
                    $object->$fieldName = $typeInfo['values'][$index];
                }
                array_push($innerArray, $object);
            }
            array_push($array, $innerArray);
        }
        $this->putGetObjectArrays(new ObjectArrayType(new ObjectArrayType($complexObjectType)), $array);
    }

    public function testPutGetObjectArrayOfObjectArraysOfComplexObjectsWithDefaultFieldTypes(): void
    {
        $array = array();
        for ($i = 0; $i < 2; $i++) {
            $innerArray = array();
            for ($j = 0; $j < 2; $j++) {
                $object = new TstComplObjectWithDefaultFieldTypes();
                foreach (TestingHelper::$primitiveValues as $type => $typeInfo) {
                    if (!array_key_exists('typeOptional', $typeInfo)) {
                        continue;
                    }
                    $fieldName = 'field' . $type;
                    $index = $i * 10 + $j;
                    $index = ($index < count($typeInfo['values'])) ? $index : $index % count($typeInfo['values']);
                    $object->$fieldName = $typeInfo['values'][$index];
                }
                array_push($innerArray, $object);
            }
            array_push($array, $innerArray);
        }
        $this->putGetObjectArrays(new ObjectArrayType(new ObjectArrayType(new ComplexObjectType())), $array);
    }

    public function testPutGetDateTime(): void
    {
        $this->putGetDate("Y-m-d H:i:s", "2018-10-19 18:31:13", 0);
        $this->putGetDate("Y-m-d H:i:s", "2018-10-19 18:31:13", 29726);
        $this->putGetDate("Y-m-d H:i:s", "2018-10-19 18:31:13", 999999);

        $this->putGetTimestamp("Y-m-d H:i:s", "2018-10-19 18:31:13", 0);
        $this->putGetTimestamp("Y-m-d H:i:s", "2018-10-19 18:31:13", 29726000);
        $this->putGetTimestamp("Y-m-d H:i:s", "2018-10-19 18:31:13", 999999999);

        $this->putGetTimestampFromDateTime("Y-m-d H:i:s", "2018-10-19 18:31:13", 0);
        $this->putGetTimestampFromDateTime("Y-m-d H:i:s", "2018-10-19 18:31:13", 29726);
        $this->putGetTimestampFromDateTime("Y-m-d H:i:s", "2018-10-19 18:31:13", 999999);
    }

    public function testPutEnumItems(): void
    {
        $fakeTypeId = 12345;
        $enumItem1 = new EnumItem($fakeTypeId);
        $enumItem1->setOrdinal(1);
        $this->putEnumItem($enumItem1, null);
        $this->putEnumItem($enumItem1, ObjectType::ENUM);
        $enumItem2 = new EnumItem($fakeTypeId);
        $enumItem2->setName('name');
        $this->putEnumItem($enumItem2, null);
        $this->putEnumItem($enumItem2, ObjectType::ENUM);
        $enumItem3 = new EnumItem($fakeTypeId);
        $enumItem3->setOrdinal(2);
        $this->putEnumItem($enumItem3, null);
        $this->putEnumItem($enumItem3, ObjectType::ENUM);
    }

    private function putEnumItem($value, $valueType): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType($valueType);
        // Enums registration is not supported by the client, therefore put EnumItem must throw ClientException
        try {
            self::$cache->put($key, $value);
            $this->fail('put EnumItem must throw ClientException');
        } catch (ClientException $e) {
            $this->assertContains('Enum item can not be serialized', $e->getMessage());
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetDate(string $format, string $dateString, int $micros): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType(ObjectType::DATE);
        try {
            $dt = DateTime::createFromFormat("$format.u", sprintf("%s.%06d", $dateString, $micros));
            $iDate = Date::fromDateTime($dt);
            self::$cache->put($key, $iDate);
            $result = self::$cache->get($key);

            $this->assertEquals(sprintf("%06d", intval($micros / 1000) * 1000), $result->toDateTime()->format('u'));
            $this->assertEquals($dateString, $result->toDateTime()->format($format));
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetTimestamp(string $format, string $dateString, int $nanos): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType(ObjectType::TIMESTAMP);

        try {
            $millis = intval($nanos / 1000000);
            $nanosInMillis = $nanos % 1000000;
            self::$cache->put($key,
                new Timestamp(
                    DateTime::createFromFormat($format, $dateString)->getTimestamp() * 1000 + $millis,
                    $nanosInMillis
                )
            );
            $result = self::$cache->get($key);

            $this->assertEquals($nanos % 1000000, $result->getNanos());
            $this->assertEquals($dateString, $result->toDateTime()->format($format));
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetTimestampFromDateTime(string $format, string $dateString, $micros): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType(ObjectType::TIMESTAMP);

        try {
            self::$cache->put($key, Timestamp::fromDateTime(
                DateTime::createFromFormat("$format.u", sprintf("%s.%06d", $dateString, $micros))
            ));
            $result = self::$cache->get($key);

            $this->assertEquals(intval($micros / 1000) * 1000, $result->toDateTime()->format('u'));
            $this->assertEquals($dateString, $result->toDateTime()->format($format));
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetObjectArrays(?ObjectArrayType $arrayType, array $value): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType($arrayType);
        try {
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            $this->assertTrue(is_array($result));
            $this->assertTrue(TestingHelper::compare($value, $result));
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetLists(?CollectionObjectType $listType, array $value): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType($listType);
        try {
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            $strResult = TestingHelper::printValue($result);
            $strValue = TestingHelper::printValue($value);
            $strValueType = TestingHelper::printValue($listType ? $listType->getElementType() : null);
            $this->assertTrue(
                is_array($result),
                "result is not array: result={$strResult}");
            $this->assertTrue(
                TestingHelper::compare($value, $result),
                "Lists are not equal: valueType={$strValueType}, put value={$strValue}, get value={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetSets(?CollectionObjectType $setType, Set $value): void
    {
        $key = microtime();
        self::$cache->
            setKeyType(null)->
            setValueType($setType);
        try {
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            $strResult = TestingHelper::printValue($result);
            $strValue = TestingHelper::printValue($value);
            $strValueType = TestingHelper::printValue($setType ? $setType->getElementType() : null);
            $this->assertTrue(
                $result instanceof Set,
                "result is not Set: result={$strResult}");
            $this->assertTrue(
                TestingHelper::compare($value, $result),
                "Sets are not equal: valueType={$strValueType}, put value={$strValue}, get value={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetArrayMaps(?MapObjectType $mapType, array $value): void
    {
        self::$cache->
            setKeyType(null)->
            setValueType($mapType);
        try {
            $key = microtime();
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            $strResult = TestingHelper::printValue($result);
            $strValue = TestingHelper::printValue($value);
            $strValueType = TestingHelper::printValue($mapType ? $mapType->getValueType() : null);
            $this->assertTrue(
                $result instanceof Map,
                "result is not Map: result={$strResult}");
            $this->assertTrue(
                TestingHelper::compare(new Map($value), $result),
                "Maps are not equal: valueType={$strValueType}, put value={$strValue}, get value={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetMaps(?MapObjectType $mapType, Map $value): void
    {
        self::$cache->
            setKeyType(null)->
            setValueType($mapType);
        try {
            $key = microtime();
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            $strResult = TestingHelper::printValue($result);
            $strValue = TestingHelper::printValue($value);
            $strValueType = TestingHelper::printValue($mapType ? $mapType->getValueType() : null);
            $this->assertTrue(
                $result instanceof Map,
                "result is not Map: result={$strResult}");
            $this->assertTrue(
                TestingHelper::compare($value, $result),
                "Maps are not equal: valueType={$strValueType}, put value={$strValue}, get value={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetPrimitiveValues(?int $typeCode1, ?int $typeCode2, $value1, $value2): void
    {
        self::$cache->
            setKeyType($typeCode1)->
            setValueType($typeCode2);
        try {
            self::$cache->put($value1, $value2);
            $result = self::$cache->get($value1);
            $strValue1 = TestingHelper::printValue($value1);
            $strValue2 = TestingHelper::printValue($value2);
            $strResult = TestingHelper::printValue($result);
            $this->assertTrue(
                TestingHelper::compare($value2, $result),
                "values are not equal: keyType={$typeCode1}, key={$strValue1}, valueType={$typeCode2}, put value={$strValue2}, get value={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }

    private function putGetArrays(int $keyType, int $valueType, $key, $value): void
    {
        self::$cache->
            setKeyType($keyType)->
            setValueType($valueType);
        try {
            self::$cache->put($key, $value);
            $result = self::$cache->get($key);
            self::$cache->clearKey($key);
            $strValue = TestingHelper::printValue($value);
            $strResult = TestingHelper::printValue($result);
            $this->assertTrue(
                is_array($result),
                "result is not Array: arrayType={$valueType}, result={$strResult}");
            $this->assertTrue(
                TestingHelper::compare($value, $result),
                "Arrays are not equal: arrayType={$valueType}, put array={$strValue}, get array={$strResult}");
        } finally {
            self::$cache->removeAll();
        }
    }

    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(self::CACHE_NAME);
    }
}
