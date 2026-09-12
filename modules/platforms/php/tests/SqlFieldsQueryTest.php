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
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Query\SqlFieldsQuery;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Type\ObjectType;

final class SqlFieldsQueryTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_sql_fields_query';
    const ELEMENTS_NUMBER = 10;
    const TABLE_NAME = '__php_test_SqlFieldsQuery_table';

    private static $cache;
    private static $selectFromTable;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        self::$cache = TestingHelper::$client->getOrCreateCache(
            self::CACHE_NAME,
            (new CacheConfiguration())->setSqlSchema('PUBLIC'));
        self::generateData();
        $tableName = self::TABLE_NAME;
        self::$selectFromTable = "SELECT * FROM {$tableName}";
    }

    public static function tearDownAfterClass(): void
    {
        self::dropTables();
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testGetAll(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query(new SqlFieldsQuery(self::$selectFromTable));
        $set = new Set();
        foreach ($cursor->getAll() as $fields) {
            $this->checkCursorResult($fields);
            $set->add($fields[0]);
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testGetAllWithPageSizeLazyTrue(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlFieldsQuery(self::$selectFromTable))->setPageSize(1)->setLazy(true));
        $set = new Set();
        foreach ($cursor->getAll() as $fields) {
            $this->checkCursorResult($fields);
            $set->add($fields[0]);
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testGetAllWithPageSizeLazyFalse(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlFieldsQuery(self::$selectFromTable))->setPageSize(1)->setLazy(false));
        $set = new Set();
        foreach ($cursor->getAll() as $fields) {
            $this->checkCursorResult($fields);
            $set->add($fields[0]);
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testIterateCursor(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query(new SqlFieldsQuery(self::$selectFromTable));
        $set = new Set();
        foreach ($cursor as $fields) {
            $this->checkCursorResult($fields);
            $set->add($fields[0]);
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testIterateCursorWithPageSizeLazyTrue(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlFieldsQuery(self::$selectFromTable))->setPageSize(2)->setLazy(true));
        $set = new Set();
        foreach ($cursor as $fields) {
            $this->checkCursorResult($fields);
            $set->add($fields[0]);
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testIterateCursorWithPageSizeLazyFalse(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlFieldsQuery(self::$selectFromTable))->setPageSize(2)->setLazy(false));
        $set = new Set();
        foreach ($cursor as $fields) {
            $this->checkCursorResult($fields);
            $set->add($fields[0]);
        }
        $this->assertEquals($set->count(), self::ELEMENTS_NUMBER);
    }

    public function testCloseCursor(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new SqlFieldsQuery(self::$selectFromTable))->setPageSize(1));
        $cursor->rewind();
        $this->assertTrue($cursor->valid());
        $this->checkCursorResult($cursor->current());
        $cursor->next();
        $cursor->close();
    }

    public function testCloseCursorAfterGetAll(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query(new SqlFieldsQuery(self::$selectFromTable));
        $cursor->getAll();
        $cursor->close();
        $this->assertTrue(true);
    }

    public function testSqlFieldsQuerySettings(): void
    {
        $tableName = self::TABLE_NAME;
        $cache = self::$cache;
        $cursor = $cache->query((new SqlFieldsQuery(self::$selectFromTable))->
            setPageSize(2)->
            setLocal(false)->
            setSql("INSERT INTO {$tableName} (field1, field2) VALUES (?, ?)")->
            setArgTypes(ObjectType::INTEGER, ObjectType::STRING)->
            setArgs(50, 'test')->
            setDistributedJoins(true)->
            setReplicatedOnly(false)->
            setTimeout(10000)->
            setSchema('PUBLIC')->
            setMaxRows(20)->
            setStatementType(SqlFieldsQuery::STATEMENT_TYPE_ANY)->
            setEnforceJoinOrder(true)->
            setCollocated(false)->
            setLazy(true)->
            setIncludeFieldNames(true));
        $cursor->getAll();
        $this->assertTrue(true);
    }

    public function testGetEmptyResults(): void
    {
        $tableName = self::TABLE_NAME;
        $cache = self::$cache;
        $cursor = $cache->query(new SqlFieldsQuery("SELECT * FROM {$tableName} WHERE field1 > 100"));
        $entries = $cursor->getAll();
        $this->assertEquals(count($entries), 0);
        $cursor->close();

        $cursor = $cache->query(new SqlFieldsQuery("SELECT * FROM {$tableName} WHERE field1 > 100"));
        foreach ($cursor as $fields) {
            $this->assertTrue(false);
        }
        $cursor->close();
    }

    private function checkCursorResult(array $fields): void
    {
        $this->assertEquals(count($fields), 2);
        $this->assertEquals($fields[1], self::generateValue($fields[0]));
        $this->assertTrue($fields[0] >= 0 && $fields[0] < self::ELEMENTS_NUMBER);
    }

    private static function dropTables(): void
    {
        $tableName = self::TABLE_NAME;
        self::$cache->query(new SqlFieldsQuery("DROP TABLE {$tableName}"))->getAll();
    }

    private static function generateData(): void
    {
        $tableName = self::TABLE_NAME;
        $cache = self::$cache;
        $cache->query(new SqlFieldsQuery(
            "CREATE TABLE IF NOT EXISTS {$tableName} (field1 INT, field2 VARCHAR, PRIMARY KEY (field1))"
        ))->getAll();
        $insertQuery = (new SqlFieldsQuery("INSERT INTO {$tableName} (field1, field2) VALUES (?, ?)"))->
            setArgTypes(ObjectType::INTEGER);

        for ($i = 0; $i < self::ELEMENTS_NUMBER; $i++) {
            $cache->query($insertQuery->setArgs($i, self::generateValue($i)))->getAll();
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
