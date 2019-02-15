<?php
/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache\Ignite\Tests;

use Apache\Ignite\Cache\CacheKeyConfiguration;
use Apache\Ignite\Cache\QueryField;
use Apache\Ignite\Cache\QueryEntity;
use Apache\Ignite\Cache\QueryIndex;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Cache\CacheInterface;
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Exception\ClientException;

final class CacheTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';
    const CACHE_NAME2 = '__php_test_cache2';
    const CACHE_NAME3 = '__php_test_cache3';
    const CACHE_NAME4 = '__php_test_cache4';

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
        $keyConfig = $cfg->getKeyConfigurations()[0];
        $queryEntity = $cfg->getQueryEntities()[0];
        $queryField = $queryEntity->getFields()[0];
        $queryIndex = $queryEntity->getIndexes()[0];
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
            setKeyConfigurations((new CacheKeyConfiguration())->
                setTypeName($keyConfig->getTypeName())->
                setAffinityKeyFieldName($keyConfig->getAffinityKeyFieldName()))->
            setQueryEntities((new QueryEntity())->
                setKeyTypeName($queryEntity->getKeyTypeName())->
                setValueTypeName($queryEntity->getValueTypeName())->
                setTableName($queryEntity->getTableName())->
                setKeyFieldName($queryEntity->getKeyFieldName())->
                setValueFieldName($queryEntity->getValueFieldName())->
                setFields(
                    (new QueryField())->
                        setName($queryField->getName())->
                        setTypeName($queryField->getTypeName())->
                        setIsKeyField($queryField->getIsKeyField())->
                        setIsNotNull($queryField->getIsNotNull())->
                        setDefaultValue($queryField->getDefaultValue())->
                        setPrecision($queryField->getPrecision())->
                        setScale($queryField->getScale()),
                    $queryEntity->getFields()[1],
                    $queryEntity->getFields()[2],
                    $queryEntity->getFields()[3])->
                setAliases($queryEntity->getAliases())->
                setIndexes((new QueryIndex())->
                    setName($queryIndex->getName())->
                    setType($queryIndex->getType())->
                    setInlineSize($queryIndex->getInlineSize())->
                    setFields($queryIndex->getFields())));
        $cache = $client->getOrCreateCache(self::CACHE_NAME4, $cfg);
        $cfg2 = $client->getCacheConfiguration(self::CACHE_NAME4);
        $client->destroyCache(self::CACHE_NAME4);
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
        TestingHelper::destroyCache(self::CACHE_NAME4);
    }
}
