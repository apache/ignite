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

use Apache\Ignite\Config\CacheKeyConfiguration;
use Apache\Ignite\Config\QueryField;
use Apache\Ignite\Config\QueryEntity;
use Apache\Ignite\Config\QueryIndex;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\CacheInterface;
use Apache\Ignite\Config\CacheConfiguration;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\ClientException;

final class CacheTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';
    const CACHE_NAME2 = '__php_test_cache2';
    const CACHE_NAME3 = '__php_test_cache3';

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testCreateCache(): void
    {
        $client = TestingHelper::$client;
        $cache = $client->getCache(self::CACHE_NAME);
        $this->checkCache($cache, false);
        $cache = $client->createCache(self::CACHE_NAME);
        $this->checkCache($cache, true);
        $cache = $client->getCache(self::CACHE_NAME);
        $this->checkCache($cache, true);
        $client->destroyCache(self::CACHE_NAME);
    }
    
    public function testCreateCacheTwice(): void
    {
        $client = TestingHelper::$client;
        try {
            $client->getOrCreateCache(self::CACHE_NAME);
            $this->expectException(OperationException::class);
            $client->createCache(self::CACHE_NAME);
        } finally {
            $client->destroyCache(self::CACHE_NAME);
        }
    }
    
    public function testGetOrCreateCache(): void
    {
        $client = TestingHelper::$client;
        $cache = $client->getCache(self::CACHE_NAME);
        $this->checkCache($cache, false);
        $cache = $client->getOrCreateCache(self::CACHE_NAME);
        $this->checkCache($cache, true);
        $cache = $client->getCache(self::CACHE_NAME);
        $this->checkCache($cache, true);
        $client->destroyCache(self::CACHE_NAME);
    }

    public function testGetCacheNames(): void
    {
        $client = TestingHelper::$client;
        $client->getOrCreateCache(self::CACHE_NAME);
        $client->getOrCreateCache(self::CACHE_NAME2);
        $cacheNames = $client->cacheNames();
        $this->assertContains(self::CACHE_NAME, $cacheNames);
        $this->assertContains(self::CACHE_NAME2, $cacheNames);
        $client->destroyCache(self::CACHE_NAME);
        $client->destroyCache(self::CACHE_NAME2);
    }
    
    public function testDestroyCache(): void
    {
        $client = TestingHelper::$client;
        $client->getOrCreateCache(self::CACHE_NAME);
        $client->destroyCache(self::CACHE_NAME);
        
        $this->expectException(OperationException::class);
        $client->destroyCache(self::CACHE_NAME);
    }
    
    public function testCreateCacheWithConfiguration(): void
    {
        $client = TestingHelper::$client;
        $cacheCfg = (new CacheConfiguration())->
            setQueryEntities(
                (new QueryEntity())->
                        setKeyTypeName('INT')->
                        setValueTypeName('Person')->
                        setTableName('Person')->
                        setKeyFieldName('id')->
                        setValueFieldName('salary')->
                        setFields(
                            (new QueryField('id', 'INT'))->setIsKeyField(true),
                            (new QueryField('firstName', 'VARCHAR'))->setIsNotNull(true),
                            (new QueryField('lastName', 'VARCHAR'))->setDefaultValue('lastName'),
                            (new QueryField('salary', 'DOUBLE'))->setPrecision(10)->setScale(10))->
                        setAliases(['id' => 'id', 'firstName' => 'firstName'])->
                        setIndexes(
                            (new QueryIndex('id_idx', QueryIndex::TYPE_SORTED))->
                                setName('id_idx')->
                                setType(QueryIndex::TYPE_SORTED)->
                                setInlineSize(10)->
                                setFields(['id' => true, 'firstName' => false])))->
            setKeyConfigurations((new CacheKeyConfiguration('Person', 'Person'))->
                setTypeName('Person')->
                setAffinityKeyFieldName('Person'));

        $cache = $client->createCache(self::CACHE_NAME3, $cacheCfg);
        $cfg = $client->getCacheConfiguration(self::CACHE_NAME3);
        $client->destroyCache(self::CACHE_NAME3);
        $cfg->
            setName($cfg->getName())->
            setAtomicityMode($cfg->getAtomicityMode())->
            setBackups($cfg->getBackups())->
            setCacheMode($cfg->getCacheMode())->
            setCopyOnRead($cfg->getCopyOnRead())->
            setDataRegionName($cfg->getDataRegionName())->
            setEagerTtl($cfg->getEagerTtl())->
            setStatisticsEnabled($cfg->getStatisticsEnabled())->
            setGroupName($cfg->getGroupName())->
            setDefaultLockTimeout($cfg->getDefaultLockTimeout())->
            setMaxConcurrentAsyncOperations($cfg->getMaxConcurrentAsyncOperations())->
            setMaxQueryIterators($cfg->getMaxQueryIterators())->
            setIsOnheapCacheEnabled($cfg->getIsOnheapCacheEnabled())->
            setPartitionLossPolicy($cfg->getPartitionLossPolicy())->
            setQueryDetailMetricsSize($cfg->getQueryDetailMetricsSize())->
            setQueryParallelism($cfg->getQueryParallelism())->
            setReadFromBackup($cfg->getReadFromBackup())->
            setRebalanceBatchSize($cfg->getRebalanceBatchSize())->
            setRebalanceBatchesPrefetchCount($cfg->getRebalanceBatchesPrefetchCount())->
            setRebalanceDelay($cfg->getRebalanceDelay())->
            setRebalanceMode($cfg->getRebalanceMode())->
            setRebalanceOrder($cfg->getRebalanceOrder())->
            setRebalanceThrottle($cfg->getRebalanceThrottle())->
            setRebalanceTimeout($cfg->getRebalanceTimeout())->
            setSqlEscapeAll($cfg->getSqlEscapeAll())->
            setSqlIndexInlineMaxSize($cfg->getSqlIndexInlineMaxSize())->
            setSqlSchema($cfg->getSqlSchema())->
            setWriteSynchronizationMode($cfg->getWriteSynchronizationMode())->
            setKeyConfigurations(...$cfg->getKeyConfigurations())->
            setQueryEntities(...$cfg->getQueryEntities());
        $cache = $client->getOrCreateCache(self::CACHE_NAME3, $cfg);
        $cfg2 = $client->getCacheConfiguration(self::CACHE_NAME3);
        $client->destroyCache(self::CACHE_NAME3);
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
        TestingHelper::destroyCache(self::CACHE_NAME);
        TestingHelper::destroyCache(self::CACHE_NAME2);
        TestingHelper::destroyCache(self::CACHE_NAME3);
    }
}
