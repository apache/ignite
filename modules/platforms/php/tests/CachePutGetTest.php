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
use Ds\Set;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\CacheInterface;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Type\MapObjectType;
use Apache\Ignite\Type\CollectionObjectType;

final class CachePutGetTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';
    
    private static $cache;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        CachePutGetTestCase::cleanUp();
        CachePutGetTestCase::$cache = TestingHelper::$client->getOrCreateCache(CachePutGetTestCase::CACHE_NAME);
    }

    public static function tearDownAfterClass(): void
    {
        CachePutGetTestCase::cleanUp();
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
    
    private function putGetSets(?CollectionObjectType $setType, ?Set $value): void
    {
        $key = microtime();
        CachePutGetTestCase::$cache->
            setKeyType(null)->
            setValueType($setType);
        try {
            CachePutGetTestCase::$cache->put($key, $value);
            $result = CachePutGetTestCase::$cache->get($key);
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
            CachePutGetTestCase::$cache->removeAll();
        }
    }
    
    private function putGetArrayMaps(?MapObjectType $mapType, ?array $value): void
    {
        CachePutGetTestCase::$cache->
            setKeyType(null)->
            setValueType($mapType);
        try {
            $key = microtime();
            CachePutGetTestCase::$cache->put($key, $value);
            $result = CachePutGetTestCase::$cache->get($key);
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
            CachePutGetTestCase::$cache->removeAll();
        }
    }
    
    private function putGetMaps(?MapObjectType $mapType, ?Map $value): void
    {
        CachePutGetTestCase::$cache->
            setKeyType(null)->
            setValueType($mapType);
        try {
            $key = microtime();
            CachePutGetTestCase::$cache->put($key, $value);
            $result = CachePutGetTestCase::$cache->get($key);
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
            CachePutGetTestCase::$cache->removeAll();
        }
    }
    
    private function putGetPrimitiveValues(?int $typeCode1, ?int $typeCode2, $value1, $value2): void
    {
        CachePutGetTestCase::$cache->
            setKeyType($typeCode1)->
            setValueType($typeCode2);
        try {
            CachePutGetTestCase::$cache->put($value1, $value2);
            $result = CachePutGetTestCase::$cache->get($value1);
            $strValue1 = TestingHelper::printValue($value1);
            $strValue2 = TestingHelper::printValue($value2);
            $strResult = TestingHelper::printValue($result);
            $this->assertTrue(
                TestingHelper::compare($value2, $result),
                "values are not equal: keyType={$typeCode1}, key={$strValue1}, valueType={$typeCode2}, put value={$strValue2}, get value={$strResult}");
        } finally {
            CachePutGetTestCase::$cache->removeAll();
        }
    }
    
    private function putGetArrays(int $keyType, int $valueType, $key, $value): void
    {
        CachePutGetTestCase::$cache->
            setKeyType($keyType)->
            setValueType($valueType);
        try {
            CachePutGetTestCase::$cache->put($key, $value);
            $result = CachePutGetTestCase::$cache->get($key);
            CachePutGetTestCase::$cache->clearKey($key);
            $strValue = TestingHelper::printValue($value);
            $strResult = TestingHelper::printValue($result);
            $this->assertTrue(
                is_array($result),
                "result is not Array: arrayType={$valueType}, result={$strResult}");
            $this->assertTrue(
                TestingHelper::compare($value, $result),
                "Arrays are not equal: arrayType={$valueType}, put array={$strValue}, get array={$strResult}");
        } finally {
            CachePutGetTestCase::$cache->removeAll();
        }
    }
    
    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(CachePutGetTestCase::CACHE_NAME);
    }
}
