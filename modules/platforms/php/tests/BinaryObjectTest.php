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
use Apache\Ignite\Data\BinaryObject;
use Apache\Ignite\Data\Date;

final class BinaryObjectTestCase extends TestCase
{
    const CACHE_NAME = '__php_test_cache';

    const TYPE_NAME = 'TestClass';
    const INNER_TYPE_NAME = 'InnerTestClass';
    const STRING_VALUE = 'abc';
    const DOUBLE_VALUE = 123.45;
    const BOOL_VALUE = false;
    const INT_VALUE = 456;
    
    private static $cache;

    public static function setUpBeforeClass(): void
    {
        TestingHelper::init();
        self::cleanUp();
        self::$cache = TestingHelper::$client->getOrCreateCache(self::CACHE_NAME);
    }

    public static function tearDownAfterClass(): void
    {
        self::cleanUp();
        TestingHelper::cleanUp();
    }
    
    public function testBinaryObjectSetGetFields(): void
    {
        $cache = self::$cache;
        try {
            $obj1 = new BinaryObject(self::TYPE_NAME);
            $obj1->setField('field_double', self::DOUBLE_VALUE);
            $obj1->setField('field_string', self::STRING_VALUE);

            $cache->put(2, $obj1);
            $cache->put(3, $obj1);
            $obj2 = $cache->get(2);
            $this->assertTrue(TestingHelper::compare($obj1, $obj2));

            $obj2->setField('field_double', $obj1->getField('field_double'));
            $obj2->setField('field_string', $obj1->getField('field_string'));
            $this->assertTrue(TestingHelper::compare($obj1, $obj2));

            $obj3 = $cache->get(3);
            $obj1->setField('field_double', $obj3->getField('field_double'));
            $obj1->setField('field_string', $obj3->getField('field_string'));
            $this->assertTrue(TestingHelper::compare($obj1, $obj3));
        } finally {
            $cache->removeAll();
        }
    }

    public function testBinaryObjectGetNonexistentField(): void
    {
        $cache = self::$cache;
        try {
            $obj1 = new BinaryObject(self::TYPE_NAME);
            $obj1->setField('field_double', self::DOUBLE_VALUE);
            $obj1->setField('field_string', self::STRING_VALUE);

            $this->expectException(ClientException::class);
            $obj1->getField('field_bool');

            $cache->put(2, $obj1);
            $obj2 = $cache->get(2);
            $obj2->getField('field_bool');
        } finally {
            $cache->removeAll();
        }
    }

    public function testBinaryObjectRemoveField(): void
    {
        $cache = self::$cache;
        try {
            $obj1 = new BinaryObject(self::TYPE_NAME);
            $obj1->setField('field_double', self::DOUBLE_VALUE);
            $obj1->setField('field_string', self::STRING_VALUE);
            $obj1->setField('field_bool', self::BOOL_VALUE);
            $this->assertTrue($obj1->hasField('field_bool'));
            $this->assertEquals($obj1->getField('field_bool'), self::BOOL_VALUE);

            $obj1->removeField('field_bool');
            $this->assertFalse($obj1->hasField('field_bool'));

            $cache->put(3, $obj1);
            $obj2 = $cache->get(3);
            $this->assertTrue(TestingHelper::compare($obj1, $obj2));

            $obj2->setField('field_bool', self::BOOL_VALUE);
            $this->assertTrue($obj2->hasField('field_bool'));
            $this->assertEquals($obj2->getField('field_bool'), self::BOOL_VALUE);

            $obj2->removeField('field_bool');
            $this->assertFalse($obj2->hasField('field_bool'));

            $obj2->setField('field_bool', self::BOOL_VALUE);
            $cache->put(4, $obj2);

            $obj1->setField('field_bool', self::BOOL_VALUE);
            $cache->put(5, $obj1);

            $this->assertTrue(TestingHelper::compare($cache->get(4), $cache->get(5)));
        } finally {
            $cache->removeAll();
        }
    }

    public function testBinaryObjectOfDifferentSchemas(): void
    {
        $cache = self::$cache;
        try {
            $obj1 = new BinaryObject(self::TYPE_NAME);
            $obj1->setField('field_int', self::INT_VALUE);
            $obj1->setField('field_string', self::STRING_VALUE);
            $obj1->setField('field_bool', self::BOOL_VALUE);
            $cache->put(1, $obj1);

            $obj2 = new BinaryObject(self::TYPE_NAME);
            $obj2->setField('field_int', self::INT_VALUE);
            $obj2->setField('field_bool', self::BOOL_VALUE);
            $obj2->setField('field_date', new Date(12345));
            $cache->put(2, $obj2);

            $obj3 = $cache->get(1);
            $obj3->removeField('field_string');
            $obj4 = $cache->get(2);
            $obj4->removeField('field_date');
            $this->assertTrue(TestingHelper::compare($obj3, $obj4));

            $cache->put(3, $obj3);
            $cache->put(4, $obj4);
            $this->assertTrue(TestingHelper::compare($cache->get(3), $cache->get(4)));
        } finally {
            $cache->removeAll();
        }
    }

    public function testNestedBinaryObjects(): void
    {
        $cache = self::$cache;
        try {
            $obj2 = new BinaryObject(self::INNER_TYPE_NAME);
            $obj2->setField('field_int', self::INT_VALUE);
            $obj2->setField('field_date', new Date(1234567));

            $obj1 = new BinaryObject(self::TYPE_NAME);
            $obj1->setField('field_double', self::DOUBLE_VALUE);
            $obj1->setField('field_string', self::STRING_VALUE);
            $obj1->setField('field_object', $obj2);

            $cache->put(1, $obj1);
            $obj3 = $cache->get(1);
            $obj4 = $obj3->getField('field_object');
            $obj4->setField('field_int', $obj4->getField('field_int') + 1);
            $obj3->setField('field_object', $obj4);
            $cache->put(3, $obj3);

            $obj2->setField('field_int', self::INT_VALUE + 1);
            $obj1->setField('field_object', $obj2);
            $this->assertTrue(TestingHelper::compare($cache->get(3), $obj1));
        } finally {
            $cache->removeAll();
        }
    }

    private static function cleanUp(): void
    {
        TestingHelper::destroyCache(self::CACHE_NAME);
    }
}
