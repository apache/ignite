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
use Apache\Ignite\CacheClientInterface;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\IgniteClientException;

final class CacheTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';
    const CACHE_NAME2 = '__php_test_cache2';

    public static function setUpBeforeClass()
    {
        TestingHelper::init();
        CacheTestCase::cleanUp();
    }

    public static function tearDownAfterClass()
    {
        CacheTestCase::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testCreateCache(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        $cache = $igniteClient->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, false);
        $cache = $igniteClient->createCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $cache = $igniteClient->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $igniteClient->destroyCache(CacheTestCase::CACHE_NAME);
    }
    
    public function testCreateCacheTwice(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        try {
            $igniteClient->getOrCreateCache(CacheTestCase::CACHE_NAME);
            $this->expectException(OperationException::class);
            $igniteClient->createCache(CacheTestCase::CACHE_NAME);
        }
        finally {
            $igniteClient->destroyCache(CacheTestCase::CACHE_NAME);
        }
    }
    
    public function testGetOrCreateCache(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        $cache = $igniteClient->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, false);
        $cache = $igniteClient->getOrCreateCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $cache = $igniteClient->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $igniteClient->destroyCache(CacheTestCase::CACHE_NAME);
    }

    public function testGetCacheNames(): void
    {
        //TODO
        $this->assertTrue(true);
    }
    
    public function testDestroyCache(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        $igniteClient->getOrCreateCache(CacheTestCase::CACHE_NAME);
        $igniteClient->destroyCache(CacheTestCase::CACHE_NAME);
        
        $this->expectException(OperationException::class);
        $igniteClient->destroyCache(CacheTestCase::CACHE_NAME);
    }
    
    public function testCreateCacheWithConfiguration(): void
    {
        //TODO
        $this->assertTrue(true);
    }
    
    public function testCreateCacheWithWrongArgs(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        $this->expectException(IgniteClientException::class);
        $igniteClient->createCache('');
    }

    public function testGetOrCreateCacheWithWrongArgs(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        $this->expectException(IgniteClientException::class);
        $igniteClient->getOrCreateCache('');
    }
    
    public function testGetCacheWithWrongArgs(): void
    {
        $igniteClient = TestingHelper::getIgniteClient();
        $this->expectException(IgniteClientException::class);
        $igniteClient->getCache('');
    }
    
    private function checkCache(CacheClientInterface $cache, bool $cacheExists)
    {
        if (!$cacheExists) {
            $this->expectException(OperationException::class);
        }
        $cache->put(0, 0);
    }
    
    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(CacheTestCase::CACHE_NAME);
        TestingHelper::destroyCache(CacheTestCase::CACHE_NAME2);
    }
}
