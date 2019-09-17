<?php
/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Ignite\Tests;

use PHPUnit\Framework\TestCase;
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Query\SqlFieldsQuery;
use Apache\Ignite\Type\ObjectType;


final class UUIDTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';
    const TABLE_NAME = '__php_test_uuid_table';
    const UUID_STRINGS = [
        'd57babad-7bc1-4c82-9f9c-e72841b92a85',
        '5946c0c0-2b76-479d-8694-a2e64a3968da',
        'a521723d-ad5d-46a6-94ad-300f850ef704'
    ];

    private static $cache;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        self::$cache = TestingHelper::$client->getOrCreateCache(
            self::CACHE_NAME,
            (new CacheConfiguration())->setSqlSchema('PUBLIC'));
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }

    public function testUUIDMarshalling(): void
    {
        $tableName = self::TABLE_NAME;
        $uuidStrings = self::UUID_STRINGS;
        $cache = self::$cache;

        $cache->query(new SqlFieldsQuery(
            "CREATE TABLE IF NOT EXISTS {$tableName} (id INTEGER PRIMARY KEY, uuid_field UUID)"
        ))->getAll();

        $insertQuery = (new SqlFieldsQuery("INSERT INTO {$tableName} (id, uuid_field) VALUES (?, ?)"))->
            setArgTypes(ObjectType::INTEGER, ObjectType::UUID);

        for ($i = 0; $i < count($uuidStrings); $i++) {
            $cache->query($insertQuery->setArgs($i, self::uuidToBytes($uuidStrings[$i])))->getAll();
        }

        $selectQuery = new SqlFieldsQuery("SELECT * FROM {$tableName} WHERE uuid_field = ?");

        foreach ($uuidStrings as $uuidStr) {
            $cursor = $cache->query($selectQuery->setArgs($uuidStr));
            $rows = $cursor->getAll();
            $this->assertEquals(count($rows), 1);
            $this->assertEquals($rows[0][1], self::uuidToBytes($uuidStr));
            $cursor->close();
        }
    }

    private function uuidToBytes($uuidStr) {
        $octets = str_split(str_replace('-', '', $uuidStr), 2);

        $hexToDecCallback = function($hex) {
            return hexdec($hex);
        };

        $res = array_map($hexToDecCallback, $octets);

        return $res;
    }

    private static function cleanUp(): void
    {
        $cache = TestingHelper::$client->getOrCreateCache(
            self::CACHE_NAME,
            (new CacheConfiguration())->setSqlSchema('PUBLIC'));
        $tableName = self::TABLE_NAME;
        $cache->query(new SqlFieldsQuery("DROP TABLE IF EXISTS {$tableName}"))->getAll();
        TestingHelper::destroyCache(self::CACHE_NAME);
    }
}
