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

final class CacheTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';
    const CACHE_NAME2 = '__php_test_cache2';

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        CacheTestCase::cleanUp();
    }

    public static function tearDownAfterClass(): void
    {
        CacheTestCase::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testCreateCache(): void
    {
        $client = TestingHelper::$client;
        $cache = $client->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, false);
        $cache = $client->createCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $cache = $client->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $client->destroyCache(CacheTestCase::CACHE_NAME);
    }
    
    public function testCreateCacheTwice(): void
    {
        $client = TestingHelper::$client;
        try {
            $client->getOrCreateCache(CacheTestCase::CACHE_NAME);
            $this->expectException(OperationException::class);
            $client->createCache(CacheTestCase::CACHE_NAME);
        } finally {
            $client->destroyCache(CacheTestCase::CACHE_NAME);
        }
    }
    
    public function testGetOrCreateCache(): void
    {
        $client = TestingHelper::$client;
        $cache = $client->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, false);
        $cache = $client->getOrCreateCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $cache = $client->getCache(CacheTestCase::CACHE_NAME);
        $this->checkCache($cache, true);
        $client->destroyCache(CacheTestCase::CACHE_NAME);
    }

    public function testGetCacheNames(): void
    {
        $client = TestingHelper::$client;
        $client->getOrCreateCache(CacheTestCase::CACHE_NAME);
        $client->getOrCreateCache(CacheTestCase::CACHE_NAME2);
        $cacheNames = $client->cacheNames();
        $this->assertContains(CacheTestCase::CACHE_NAME, $cacheNames);
        $this->assertContains(CacheTestCase::CACHE_NAME2, $cacheNames);
        $client->destroyCache(CacheTestCase::CACHE_NAME);
        $client->destroyCache(CacheTestCase::CACHE_NAME2);
    }
    
    public function testDestroyCache(): void
    {
        $client = TestingHelper::$client;
        $client->getOrCreateCache(CacheTestCase::CACHE_NAME);
        $client->destroyCache(CacheTestCase::CACHE_NAME);
        
        $this->expectException(OperationException::class);
        $client->destroyCache(CacheTestCase::CACHE_NAME);
    }
    
    public function testCreateCacheWithConfiguration(): void
    {
        //TODO
        $this->assertTrue(true);
    }
    
    public function testCreateCacheWithWrongArgs(): void
    {
        $client = TestingHelper::$client;
        $this->expectException(ClientException::class);
        $client->createCache('');
    }

    public function testGetOrCreateCacheWithWrongArgs(): void
    {
        $client = TestingHelper::$client;
        $this->expectException(ClientException::class);
        $client->getOrCreateCache('');
    }
    
    public function testGetCacheWithWrongArgs(): void
    {
        $client = TestingHelper::$client;
        $this->expectException(ClientException::class);
        $client->getCache('');
    }
    
    private function checkCache(CacheInterface $cache, bool $cacheExists): void
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
