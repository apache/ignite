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
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Cache\CacheEntry;
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Cache\CacheInterface;

final class CacheKeyValueOpsTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        TestingHelper::$client->getOrCreateCache(self::CACHE_NAME);
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testCacheGet(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $value = $cache->get(1);
            $this->assertEquals($value, null);
            $cache->put(1, 2);
            $value = $cache->get(1);
            $this->assertEquals($value, 2);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function cacheWrongKeys(): array
    {
        return array(
            array(null)
        );
    }

    /**
     * @dataProvider cacheWrongKeys
     */
    public function testCacheGetWrongArgs($cacheKey): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->get($cacheKey);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testCacheGetAll(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            for ($i = 0; $i < 5; $i++) {
                $cache->put($i, $i * 2);
            }
            $entries = $cache->getAll([3, 4, 5, 6, 7]);
            $this->assertCount(2, $entries);
            foreach ($entries as $entry) {
                $this->assertTrue($entry->getKey() === 3 || $entry->getKey() === 4);
                $this->assertEquals($entry->getValue(), $entry->getKey() * 2);
            }
            $entries = $cache->getAll([6, 7, 8]);
            $this->assertCount(0, $entries);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function cacheGetAllWrongKeys(): array
    {
        return array(
            array([])
        );
    }

    /**
     * @dataProvider cacheGetAllWrongKeys
     */
    public function testCacheGetAllWrongArgs($keys): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->getAll($keys);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCachePut(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $value = $cache->get(1);
            $this->assertEquals($value, null);
            $cache->put(1, 2);
            $value = $cache->get(1);
            $this->assertEquals($value, 2);
            $cache->put(1, 4);
            $value = $cache->get(1);
            $this->assertEquals($value, 4);
        } finally {
            $cache->removeAll();
        }
    }

    public function cacheWrongKeyValues(): array
    {
        return array(
            array(null, 1),
            array(1, null),
            array(null, null)
        );
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testCachePutWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->put($key, $value);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCachePutAll(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $cacheEntries = array();
            for ($i = 0; $i < 5; $i++) {
                array_push($cacheEntries, new CacheEntry($i, $i * 2));
            }
            $cache->putAll($cacheEntries);
            $entries = $cache->getAll([3, 4, 5, 6, 7]);
            $this->assertCount(2, $entries);
            foreach ($entries as $entry) {
                $this->assertTrue($entry->getKey() === 3 || $entry->getKey() === 4);
                $this->assertEquals($entry->getValue(), $entry->getKey() * 2);
            }
            $entries = $cache->getAll([-2, -1, 0, 1, 2, 3, 4, 5, 6, 7]);
            $this->assertCount(5, $entries);
            foreach ($entries as $entry) {
                $this->assertTrue($entry->getKey() >= 0 && $entry->getKey() <= 4);
                $this->assertEquals($entry->getValue(), $entry->getKey() * 2);
            }
            $entries = $cache->getAll([6, 7, 8]);
            $this->assertCount(0, $entries);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function cachePutAllWrongArgs(): array
    {
        return array(
            array([]),
            array([new CacheConfiguration])
        );
    }

    /**
     * @dataProvider cachePutAllWrongArgs
     */
    public function testCachePutAllWrongArgs($entries): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->putAll($entries);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testContainsKey(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->containsKey(1);
            $this->assertFalse($result);
            $cache->put(1, 2);
            $result = $cache->containsKey(1);
            $this->assertTrue($result);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheWrongKeys
     */
    public function testContainsKeyWrongArgs($cacheKey): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->containsKey($cacheKey);
        } finally {
            $cache->removeAll();
        }
    }
  
    public function testCacheContainsKeys(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->containsKeys([1, 2, 3]);
            $this->assertFalse($result);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4)]);
            $result = $cache->containsKeys([1, 2, 3]);
            $this->assertFalse($result);
            $cache->put(3, 6);
            $result = $cache->containsKeys([1, 2, 3]);
            $this->assertTrue($result);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheGetAllWrongKeys
     */
    public function testCacheContainsKeysWrongArgs($keys): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->containsKeys($keys);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testCacheGetAndPut(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $value = $cache->getAndPut(1, 2);
            $this->assertEquals($value, null);
            $value = $cache->getAndPut(1, 4);
            $this->assertEquals($value, 2);
            $cache->put(1, 6);
            $value = $cache->getAndPut(1, 8);
            $this->assertEquals($value, 6);
            $value = $cache->get(1);
            $this->assertEquals($value, 8);

        } finally {
            $cache->removeAll();
        }
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testCacheGetAndPutWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->getAndPut($key, $value);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCacheGetAndReplace(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $value = $cache->getAndReplace(1, 2);
            $this->assertEquals($value, null);
            $cache->put(1, 4);
            $value = $cache->getAndReplace(1, 6);
            $this->assertEquals($value, 4);
            $value = $cache->get(1);
            $this->assertEquals($value, 6);
        } finally {
            $cache->removeAll();
        }
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testCacheGetAndReplaceWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->getAndReplace($key, $value);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testCacheGetAndRemove(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $value = $cache->getAndRemove(1);
            $this->assertEquals($value, null);
            $cache->put(1, 2);
            $value = $cache->getAndRemove(1);
            $this->assertEquals($value, 2);
            $value = $cache->get(1);
            $this->assertEquals($value, null);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheWrongKeys
     */
    public function testCacheGetAndRemoveWrongArgs($cacheKey): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->getAndRemove($cacheKey);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCachePutIfAbsent(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->putIfAbsent(1, 2);
            $this->assertTrue($result);
            $value = $cache->get(1);
            $this->assertEquals($value, 2);
            $result = $cache->putIfAbsent(1, 4);
            $this->assertFalse($result);
            $value = $cache->get(1);
            $this->assertEquals($value, 2);
        } finally {
            $cache->removeAll();
        }
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testCachePutIfAbsentWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->putIfAbsent($key, $value);
        } finally {
            $cache->removeAll();
        }
    }
  
    public function testCacheGetAndPutIfAbsent(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $value = $cache->getAndPutIfAbsent(1, 2);
            $this->assertEquals($value, null);
            $value = $cache->get(1);
            $this->assertEquals($value, 2);
            $value = $cache->getAndPutIfAbsent(1, 4);
            $this->assertEquals($value, 2);
            $value = $cache->get(1);
            $this->assertEquals($value, 2);

        } finally {
            $cache->removeAll();
        }
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testCacheGetAndPutIfAbsentWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->getAndPutIfAbsent($key, $value);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCacheReplace(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->replace(1, 2);
            $this->assertFalse($result);
            $value = $cache->get(1);
            $this->assertEquals($value, null);
            $cache->put(1, 1);
            $result = $cache->replace(1, 4);
            $this->assertTrue($result);
            $value = $cache->get(1);
            $this->assertEquals($value, 4);

        } finally {
            $cache->removeAll();
        }
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testCacheReplaceWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->replace($key, $value);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testCacheReplaceIfEquals(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->replaceIfEquals(1, 2, 3);
            $this->assertFalse($result);
            $cache->put(1, 4);
            $result = $cache->replaceIfEquals(1, 2, 3);
            $this->assertFalse($result);
            $value = $cache->get(1);
            $this->assertEquals($value, 4);
            $result = $cache->replaceIfEquals(1, 4, 3);
            $this->assertTrue($result);
            $value = $cache->get(1);
            $this->assertEquals($value, 3);
        } finally {
            $cache->removeAll();
        }
    }

    public function cacheWrongReplaceIfEqualsArgs(): array
    {
        return array(
            array(null, 1, 1),
            array(null, 1, null),
            array(1, null, 1),
            array(1, null, null),
            array(null, null, null)
        );
    }

    /**
     * @dataProvider cacheWrongReplaceIfEqualsArgs
     */
    public function testCacheReplaceIfEqualsWrongArgs($key, $value, $newValue): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->replaceIfEquals($key, $value, $newValue);
        } finally {
            $cache->removeAll();
        }
    }
  
    public function testCacheClear(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $cache->clear();
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $cache->clear();
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $entries = $cache->getAll([1, 2, 3]);
            $this->assertCount(0, $entries);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCacheClearKeys(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $cache->clearKeys([1, 2, 3]);
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $cache->clearKeys([1, 2, 7, 8]);
            $result = $cache->getSize();
            $this->assertEquals($result, 1);
            $value = $cache->get(3);
            $this->assertEquals($value, 6);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheGetAllWrongKeys
     */
    public function testCacheClearKeysWrongArgs($keys): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->clearKeys($keys);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCacheClearKey(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $cache->clearKey(1);
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $cache->clearKey(1);
            $result = $cache->getSize();
            $this->assertEquals($result, 2);
            $value = $cache->get(2);
            $this->assertEquals($value, 4);
            $value = $cache->get(3);
            $this->assertEquals($value, 6);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheWrongKeys
     */
    public function testCacheClearKeyWrongArgs($cacheKey): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->clearKey($cacheKey);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCacheRemoveKey(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->removeKey(1);
            $this->assertFalse($result);
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $result = $cache->removeKey(1);
            $this->assertTrue($result);
            $result = $cache->removeKey(1);
            $this->assertFalse($result);
            $result = $cache->getSize();
            $this->assertEquals($result, 2);
            $value = $cache->get(2);
            $this->assertEquals($value, 4);
            $value = $cache->get(3);
            $this->assertEquals($value, 6);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheWrongKeys
     */
    public function testCacheRemoveKeyWrongArgs($cacheKey): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->removeKey($cacheKey);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testRemoveIfEquals(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->removeIfEquals(1, 2);
            $this->assertFalse($result);
            $cache->put(1, 4);
            $result = $cache->removeIfEquals(1, 2);
            $this->assertFalse($result);
            $result = $cache->removeIfEquals(1, 4);
            $this->assertTrue($result);
            $value = $cache->get(1);
            $this->assertEquals($value, null);
        } finally {
            $cache->removeAll();
        }
    }

    /**
     * @dataProvider cacheWrongKeyValues
     */
    public function testRemoveIfEqualsWrongArgs($key, $value): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->removeIfEquals($key, $value);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testCacheRemoveKeys(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $cache->removeKeys([1, 2, 3]);
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $cache->removeKeys([1, 2, 7, 8]);
            $result = $cache->getSize();
            $this->assertEquals($result, 1);
            $value = $cache->get(3);
            $this->assertEquals($value, 6);
        } finally {
            $cache->removeAll();
        }
    }
    
    /**
     * @dataProvider cacheGetAllWrongKeys
     */
    public function testCacheRemoveKeysWrongArgs($keys): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->removeKeys($keys);
        } finally {
            $cache->removeAll();
        }
    }

    public function testCacheRemoveAll(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $cache->removeAll();
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $cache->removeAll();
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $entries = $cache->getAll([1, 2, 3]);
            $this->assertCount(0, $entries);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function testCacheGetSize(): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $result = $cache->getSize();
            $this->assertEquals($result, 0);
            $cache->putAll([new CacheEntry(1, 2), new CacheEntry(2, 4), new CacheEntry(3, 6)]);
            $result = $cache->getSize();
            $this->assertEquals($result, 3);
            $result = $cache->getSize(CacheInterface::PEEK_MODE_ALL);
            $this->assertEquals($result, 3);
            $result = $cache->getSize(CacheInterface::PEEK_MODE_ALL, CacheInterface::PEEK_MODE_ALL);
            $this->assertEquals($result, 3);
        } finally {
            $cache->removeAll();
        }
    }
    
    public function getSizeWrongArgs(): array
    {
        return array(
            array(-1, -2),
            array(4, 5, 6, 0)
        );
    }

    /**
     * @dataProvider getSizeWrongArgs
     */
    public function testCacheGetSizeWrongArgs($modes): void
    {
        $cache = TestingHelper::$client->getCache(self::CACHE_NAME);
        try {
            $this->expectException(ClientException::class);
            $cache->getSize($modes);
        } finally {
            $cache->removeAll();
        }
    }
    
    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(self::CACHE_NAME);
    }
}
