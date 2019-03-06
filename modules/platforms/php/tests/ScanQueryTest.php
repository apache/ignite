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

use Ds\Set;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Query\ScanQuery;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Cache\CacheEntry;

final class ScanQueryTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache_scan_query';
    const CACHE_NAME2 = '__php_test_cache_scan_query_2';
    const ELEMENTS_NUMBER = 10;

    private static $cache;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        self::$cache = TestingHelper::$client->getOrCreateCache(self::CACHE_NAME);
        self::generateData();
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testGetAll(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query(new ScanQuery());
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
        $cursor = $cache->query((new ScanQuery())->setPageSize(1));
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
        $cursor = $cache->query(new ScanQuery());
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
        $cursor = $cache->query((new ScanQuery())->setPageSize(2));
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
        $cursor = $cache->query((new ScanQuery())->setPageSize(1));
        $cursor->rewind();
        $this->assertTrue($cursor->valid());
        $this->checkCursorResult($cursor->current());
        $cursor->next();
        $cursor->close();
    }

    public function testCloseCursorAfterGetAll(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new ScanQuery())->setPageSize(1));
        $cursor->getAll();
        $cursor->close();
        $this->assertTrue(true);
    }

    public function testScanQuerySettings(): void
    {
        $cache = self::$cache;
        $cursor = $cache->query((new ScanQuery())->
            setPartitionNumber(0)->
            setPageSize(2)->
            setLocal(true));
        $cursor->getAll();
        $this->assertTrue(true);
    }

    public function testScanEmptyCache(): void
    {
        $cache = TestingHelper::$client->getOrCreateCache(self::CACHE_NAME2);
        $cache->removeAll();
        $cursor = $cache->query(new ScanQuery());
        $cacheEntries = $cursor->getAll();
        $this->assertEquals(count($cacheEntries), 0);

        $cursor = $cache->query(new ScanQuery());
        foreach ($cursor as $entry) {
            $this->assertTrue(false);
        }
        $cursor->close();
    }

    private function checkCursorResult(CacheEntry $cacheEntry): void
    {
        $this->assertEquals($cacheEntry->getValue(), self::generateValue($cacheEntry->getKey()));
        $this->assertTrue($cacheEntry->getKey() >= 0 && $cacheEntry->getKey() < self::ELEMENTS_NUMBER);
    }

    private static function generateData(): void
    {
        $cache = self::$cache;
        $cache->setKeyType(ObjectType::INTEGER);
        for ($i = 0; $i < self::ELEMENTS_NUMBER; $i++) {
            $cache->put($i, self::generateValue($i));
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
