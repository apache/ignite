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

use Ds\Set;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Cache\CacheEntry;
use Apache\Ignite\Cache\QueryEntity;
use Apache\Ignite\Cache\QueryField;
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Query\SqlQuery;
use Apache\Ignite\Query\SqlFieldsQuery;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\ComplexObjectType;

class SqlQueryTestClass
{
    public $field1;
    public $field2;
}

final class SqlQueryTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_sql_query';
    const ELEMENTS_NUMBER = 10;
    const TABLE_NAME = '__php_test_SqlQuery_table';

    private static $cache;
    private static $selectFromTable;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        self::$cache = (TestingHelper::$client->getOrCreateCache(
            self::CACHE_NAME,
            (new CacheConfiguration())->
                setQueryEntities((new QueryEntity())->
                    setKeyTypeName('java.lang.Integer')->
                    setValueTypeName(self::TABLE_NAME)->
                    setFields(
                        new QueryField('field1', 'java.lang.Integer'),
                        new QueryField('field2', 'java.lang.String')))))->
            setKeyType(ObjectType::INTEGER)->
            setValueType((new ComplexObjectType())->
            setIgniteTypeName(self::TABLE_NAME)->
            setPhpClassName(SqlQueryTestClass::class)->
            setFieldType('field1', ObjectType::INTEGER));
        self::generateData();
        $tableName = self::TABLE_NAME;
        self::$selectFromTable = "SELECT * FROM {$tableName}";
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testGetAll(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, self::$selectFromTable)));
        $set = new Set();
        foreach ($cursor->getAll() as $cacheEntry) {
            $this->checkCursorResult($cacheEntry);
            $set->add($cacheEntry->getKey());
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testGetAllWithPageSize(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, self::$selectFromTable))->setPageSize(1));
        $set = new Set();
        foreach ($cursor->getAll() as $cacheEntry) {
            $this->checkCursorResult($cacheEntry);
            $set->add($cacheEntry->getKey());
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testIterateCursor(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query(new SqlQuery(self::TABLE_NAME, self::$selectFromTable));
        $set = new Set();
        foreach ($cursor as $cacheEntry) {
            $this->checkCursorResult($cacheEntry);
            $set->add($cacheEntry->getKey());
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testIterateCursorWithPageSize(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, self::$selectFromTable))->setPageSize(2));
        $set = new Set();
        foreach ($cursor as $cacheEntry) {
            $this->checkCursorResult($cacheEntry);
            $set->add($cacheEntry->getKey());
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testCloseCursor(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, self::$selectFromTable))->setPageSize(1));
        $cursor->rewind();
        $this->assertTrue($cursor->valid());
        $this->checkCursorResult($cursor->current());
        $cursor->next();
        $cursor->close();
    }

    public function testCloseCursorAfterGetAll(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query(new SqlQuery(self::TABLE_NAME, self::$selectFromTable));
        $cursor->getAll();
        $cursor->close();
        $this->assertTrue(true);
    }

    public function testSqlQuerySettings(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, self::$selectFromTable))->
            setType(self::TABLE_NAME)->
            setPageSize(2)->
            setLocal(false)->
            setSql('field1 > ? and field1 <= ?')->
            setArgTypes(ObjectType::INTEGER, ObjectType::INTEGER)->
            setArgs(3, 7)->
            setDistributedJoins(true)->
            setReplicatedOnly(false)->
            setTimeout(10000));
        $cursor->getAll();
        $this->assertTrue(true);
    }

    public function testGetEmptyResults(): void
    {
        $tableName = self::TABLE_NAME;
        $cache = self::$cache;
        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, 'field1 > ?'))->setArgs(self::ELEMENTS_NUMBER));
        $entries = $cursor->getAll();
        $this->assertEquals(count($entries), 0);
        $cursor->close();

        $cursor = $cache->query((new SqlQuery(self::TABLE_NAME, 'field1 > ?'))->setArgs(self::ELEMENTS_NUMBER));
        foreach ($cursor as $entry) {
            $this->assertTrue(false);
        }
        $cursor->close();
    }

    private function checkCursorResult(CacheEntry $cacheEntry): void
    {
        $this->assertEquals($cacheEntry->getValue()->field2, self::generateValue($cacheEntry->getKey()));
        $this->assertTrue($cacheEntry->getKey() >= 0 && $cacheEntry->getKey() < self::ELEMENTS_NUMBER);
    }

    private static function generateData(): void
    {
        $tableName = self::TABLE_NAME;
        $cache = self::$cache;
        $insertQuery = (new SqlFieldsQuery("INSERT INTO {$tableName} (_key, field1, field2) VALUES (?, ?, ?)"))->
            setArgTypes(ObjectType::INTEGER, ObjectType::INTEGER);

        for ($i = 0; $i < self::ELEMENTS_NUMBER; $i++) {
            $cache->query($insertQuery->setArgs($i, $i, self::generateValue($i)))->getAll();
        }
    }

    private static function generateValue(int $key): string
    {
        return 'value' . $key;
    }

    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(self::CACHE_NAME);
    }
}
