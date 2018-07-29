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

use PHPUnit\Framework\TestCase;
use Apache\Ignite\CacheInterface;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\ClientException;

final class CachePutGetTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';

    public static function setUpBeforeClass()
    {
        TestingHelper::init();
        CachePutGetTestCase::cleanUp();
        TestingHelper::$client->getOrCreateCache(CacheTestCase::CACHE_NAME);
    }

    public static function tearDownAfterClass()
    {
        CachePutGetTestCase::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testPutGetPrimitiveValues(): void
    {
        $client = TestingHelper::$client;
        $cache = $client->getCache(CacheTestCase::CACHE_NAME);
        foreach (TestingHelper::$primitiveValues as $typeCode1 => $typeInfo1) {
            foreach (TestingHelper::$primitiveValues as $typeCode2 => $typeInfo2) {
                foreach ($typeInfo1['values'] as $value1) {
                    foreach ($typeInfo2['values'] as $value2) {
                        $this->putGetPrimitiveValues($cache, $typeCode1, $typeCode2, $value1, $value2);
                        if (array_key_exists('typeOptional', $typeInfo1)) {
                            $this->putGetPrimitiveValues($cache, null, $typeCode2, $value1, $value2);
                        }
                        if (array_key_exists('typeOptional', $typeInfo2)) {
                            $this->putGetPrimitiveValues($cache, $typeCode1, null, $value1, $value2);
                        }
                    }
                }
            }
        }
    }
    
    private function putGetPrimitiveValues(CacheInterface $cache, ?int $typeCode1, ?int $typeCode2, $value1, $value2)
    {
        $cache->
            setKeyType($typeCode1)->
            setValueType($typeCode2);
        try {
            $cache->put($value1, $value2);
            $result = $cache->get($value1);
            $strValue1 = TestingHelper::printValue($value1);
            $strValue2 = TestingHelper::printValue($value2);
            $strResult = TestingHelper::printValue($result);
            $this->assertTrue(
                TestingHelper::compare($value2, $result),
                "values are not equal: keyType={$typeCode1}, key={$strValue1}, valueType={$typeCode2}, put value={$strValue2}, get value={$strResult}");
        } finally {
            $cache->removeAll();
        }
    }
    
    private function checkCache(CacheInterface $cache, bool $cacheExists)
    {
        if (!$cacheExists) {
            $this->expectException(OperationException::class);
        }
        $cache->put(0, 0);
    }
    
    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(CacheTestCase::CACHE_NAME);
    }
}
