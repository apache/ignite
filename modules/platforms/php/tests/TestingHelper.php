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

use \DateTime;
use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;

/**
 * Helper class for testing apache-ignite-client library.
 * Contains common methods for testing environment initialization and cleanup.
 */
class TestingHelper
{
    public static $client;
    public static $primitiveValues;
    
    /**
     * Initializes testing environment: creates and starts the library client, sets default jasmine test timeout.
     * Should be called from any test case setUpBeforeClass method.
     */
    public static function init(): void
    {
        TestingHelper::$client = new Client();
        TestingHelper::$client->connect(new ClientConfiguration(...TestConfig::$endpoints));
        TestingHelper::$client->setDebug(TestConfig::$debug);
        TestingHelper::initValues();
    }
    
    /**
     * Cleans up testing environment.
     * Should be called from any test case tearDownAfterClass method.
     */
    public static function cleanUp(): void
    {
        TestingHelper::$client->disconnect();
    }

    public static function destroyCache(string $cacheName): void
    {
        try {
            TestingHelper::$client->destroyCache($cacheName);
        } catch (OperationException $e) {
            //expected exception
        }
    }
    
    public static function compare($value1, $value2): bool
    {
        TestingHelper::logDebug(sprintf('compare: %s and %s', TestingHelper::printValue($value1), TestingHelper::printValue($value2)));
        if ($value1 === null && $value2 === null) {
            return true;
        } elseif ($value1 === null && $value2 !== null || $value1 !== null && $value2 === null) {
            return false;
        } elseif (gettype($value1) === 'double' || gettype($value2) === 'double') {
            return TestingHelper::floatComparator($value1, $value2);
        } elseif (gettype($value1) !== gettype($value2)) {
            TestingHelper::logDebug(sprintf('compare: value types are different: %s and %s', gettype($value1), gettype($value2)));
            return false;
        } elseif (gettype($value1) !== 'object') {
            return TestingHelper::defaultComparator($value1, $value2);
        } elseif ($value1 instanceof Time && $value2 instanceof Time) {
            return TestingHelper::defaultComparator($value1->getMillis(), $value2->getMillis());
        } elseif ($value1 instanceof Timestamp && $value2 instanceof Timestamp) {
            return TestingHelper::floatComparator($value1->getMillis(), $value2->getMillis()) &&
                   TestingHelper::defaultComparator($value1->getNanos(), $value2->getNanos());
        } elseif ($value1 instanceof Date && $value2 instanceof Date) {
            return TestingHelper::floatComparator($value1->getMillis(), $value2->getMillis());
        }
        return false;
    }
    
    private static function initValues(): void
    {
        TestingHelper::$primitiveValues = [
            ObjectType::BYTE => [
                'values' => [-128, 0, 127],
                'isMapKey' => true
            ],
            ObjectType::SHORT => [
                'values' => [-32768, 0, 32767],
                'isMapKey' => true
            ],
            ObjectType::INTEGER => [
                'values' => [12345, 0, -54321],
                'isMapKey' => true,
                'typeOptional' => true
            ],
            ObjectType::LONG => [
                'values' => [12345678912345, 0, -98765432112345],
                'isMapKey' => true
            ],
            ObjectType::FLOAT => [
                'values' => [-1.155, 0, 123e-5],
                'isMapKey' => false
            ],
            ObjectType::DOUBLE => [
                'values' => [-123e5, 0, 0.0001],
                'isMapKey' => false,
                'typeOptional' => true
            ],
            ObjectType::CHAR => [
                'values' => ['a', "\xC3\xA1"],
                'isMapKey' => true,
            ],
            ObjectType::BOOLEAN => [
                'values' => [true, false],
                'isMapKey' => true,
                'typeOptional' => true
            ],
            ObjectType::STRING => [
                'values' => ['abc', '', '123'],
                'isMapKey' => true,
                'typeOptional' => true
            ],
            ObjectType::DATE => [
                'values' => [
                    Date::fromDateTime(new DateTime('now')),
                    new Date(1234567890),
                    new Date(0)],
                'isMapKey' => true,
                'typeOptional' => true
            ],
            ObjectType::TIME => [
                'values' => [
                    new Time(1234567),
                    new Time(123)],
                'isMapKey' => true,
                'typeOptional' => true
            ],
            ObjectType::TIMESTAMP => [
                'values' => [
                    Timestamp::fromDateTime(new DateTime('now')),
                    new Timestamp(12345, 12345),
                    new Timestamp(0, 0)],
                'isMapKey' => true,
                'typeOptional' => true
            ],
        ];
    }

    private static function floatComparator(float $value1, float $value2): bool
    {
        return abs($value1 - $value2) < 0.0001;
    }

    private static function defaultComparator($value1, $value2): bool
    {
        return $value1 === $value2;
    }
    
    private static function dateComparator(Date $value1, Date $value2): bool
    {
        return !$value1 && !$value1 || $value1->getTimestamp() === $value2->getTimestamp();
    }
    
    public static function printValue($value)
    {
        if ($value instanceof Date) {
            return $value->toDateTime()->format('Y-m-d H:i:s');
        } elseif ($value instanceof Time) {
            $dateTime = new DateTime();
            $dateTime->setTimestamp($value->getSeconds());
            return $dateTime->format('H:i:s');
        } else {
            return $value;
        }
    }
    
    private static function logDebug($message): void
    {
        if (TestConfig::$debug) {
            echo($message . "\n");
        }
    }
}