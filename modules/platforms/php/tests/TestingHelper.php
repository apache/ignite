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
use Ds\Map;
use Ds\Set;
use Brick\Math\BigDecimal;
use PHPUnit\Framework\TestCase;
use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Exception\OperationException;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Data\Date;
use Apache\Ignite\Data\Time;
use Apache\Ignite\Data\Timestamp;
use Apache\Ignite\Data\BinaryObject;

/**
 * Helper class for testing apache-ignite-client library.
 * Contains common methods for testing environment initialization and cleanup.
 */
class TestingHelper
{
    public static $client;
    public static $primitiveValues;
    public static $arrayValues;
    
    /**
     * Initializes testing environment: creates and starts the library client, sets default jasmine test timeout.
     * Should be called from any test case setUpBeforeClass method.
     */
    public static function init(): void
    {
        TestingHelper::$client = new Client();
        TestingHelper::$client->setDebug(TestConfig::$debug);
        TestingHelper::$client->connect(new ClientConfiguration(...TestConfig::$endpoints));
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
    
    public static function executeExample(string $name, TestCase $testCase, array $outputChecker): void
    {
        $output = null;
        $return_var = 0;
        exec('php ' . __DIR__ . '/../examples/' . $name, $output, $return_var);
        TestingHelper::logDebug(print_r($output, true));
        $testCase->assertEquals($return_var, 0);
        foreach ($output as $out) {
            call_user_func($outputChecker, $output);
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
            TestingHelper::logDebug(sprintf('compare: value types are different: %s and %s',
                gettype($value1), gettype($value2)));
            return false;
        } elseif (is_array($value1) && is_array($value2)) {
            if (count($value1) !== count($value2)) {
                TestingHelper::logDebug('compare: array lengths are different');
                return false;
            }
            foreach ($value1 as $key => $val1) {
                $val2 = $value2[$key];
                if (!TestingHelper::compare($val1, $val2)) {
                    TestingHelper::logDebug(sprintf('compare: array elements are different: %s, %s',
                        TestingHelper::printValue($val1), TestingHelper::printValue($val2)));
                    return false;
                }
            }
            return true;
        } elseif (gettype($value1) !== 'object') {
            return TestingHelper::defaultComparator($value1, $value2);
        } elseif ($value1 instanceof Time && $value2 instanceof Time) {
            return TestingHelper::defaultComparator($value1->getMillis(), $value2->getMillis());
        } elseif ($value1 instanceof Timestamp && $value2 instanceof Timestamp) {
            return TestingHelper::floatComparator($value1->getMillis(), $value2->getMillis()) &&
                   TestingHelper::defaultComparator($value1->getNanos(), $value2->getNanos());
        } elseif ($value1 instanceof Date && $value2 instanceof Date) {
            return TestingHelper::floatComparator($value1->getMillis(), $value2->getMillis());
        } elseif ($value1 instanceof BigDecimal && $value2 instanceof BigDecimal) {
            return $value1->isEqualTo($value2);
        } elseif ($value1 instanceof Map && $value2 instanceof Map) {
            if ($value1->count() !== $value2->count()) {
                TestingHelper::logDebug('compare: map sizes are different');
                return false;
            }
            foreach ($value1->pairs() as $pair1) {
                if (!$value2->hasKey($pair1->key)) {
                    TestingHelper::logDebug(sprintf('compare: maps are different: %s key is absent',
                        TestingHelper::printValue($pair1->key)));
                    return false;
                }
                if (!TestingHelper::compare($pair1->value, $value2->get($pair1->key))) {
                    TestingHelper::logDebug(sprintf('compare: map values are different: %s, %s',
                        TestingHelper::printValue($pair1->value), TestingHelper::printValue($value2->get($pair1->key))));
                    return false;
                }
            }
            return true;
        } elseif ($value1 instanceof Set && $value2 instanceof Set) {
            if ($value1->count() !== $value2->count()) {
                TestingHelper::logDebug('compare: set sizes are different');
                return false;
            }
            $value1Arr = $value1->toArray();
            $value2Arr = $value2->toArray();
            sort($value1Arr);
            sort($value2Arr);
            if (!TestingHelper::compare($value1Arr, $value2Arr)) {
                TestingHelper::logDebug(sprintf('compare: sets are different: %s and %s',
                    TestingHelper::printValue($value1Arr), TestingHelper::printValue($value2Arr)));
                return false;
            }
            return true;
        } elseif ($value2 instanceof BinaryObject) {
            if ($value1 instanceof BinaryObject) {
                if ($value1->getTypeName() !== $value2->getTypeName()) {
                    TestingHelper::logDebug(sprintf('compare: binary object type names are different'));
                    return false;
                }
                if (!TestingHelper::compare($value1->getFieldNames(), $value2->getFieldNames())) {
                    TestingHelper::logDebug(sprintf('compare: binary object field names are different'));
                    return false;
                }
                foreach ($value1->getFieldNames() as $fieldName) {
                    if (!$value1->hasField($fieldName) || !$value2->hasField($fieldName) ||
                        !TestingHelper::compare($value1->getField($fieldName), $value2->getField($fieldName))) {
                        TestingHelper::logDebug(sprintf('compare: binary objects field "%s" values are different', $fieldName));
                        return false;
                    }
                }
                return true;
            } else {
                $reflect1 = new \ReflectionClass($value1);
                $properties1 = $reflect1->getProperties(\ReflectionProperty::IS_PUBLIC);
                foreach ($properties1 as $property1) {
                    if ($property1->isStatic()) {
                        continue;
                    }
                    $propName = $property1->getName();
                    $propValue1 = $property1->getValue($value1);
                    $propValue2 = $value2->getField($propName);
                    if (!TestingHelper::compare($propValue1, $propValue2)) {
                        TestingHelper::logDebug(sprintf('compare: binary object values for field %s are different: %s and %s',
                            TestingHelper::printValue($propName), TestingHelper::printValue($propValue1), TestingHelper::printValue($propValue2)));
                        return false;
                    }
                }
                return true;
            }
        } else {
            $reflect1 = new \ReflectionClass($value1);
            $properties1 = $reflect1->getProperties(\ReflectionProperty::IS_PUBLIC);
            $reflect2 = new \ReflectionClass($value1);
            foreach ($properties1 as $property1) {
                if ($property1->isStatic()) {
                    continue;
                }
                $propName = $property1->getName();
                if ($reflect2->hasProperty($propName)) {
                    $property2 = $reflect2->getProperty($propName);
                    if ($property1->getModifiers() !== $property2->getModifiers()) {
                        TestingHelper::logDebug(sprintf(
                            'compare: objects are different: object property modifiers for property %s are different', $propName));
                        return false;
                    } else {
                        $propValue1 = $property1->getValue($value1);
                        $propValue2 = $property2->getValue($value2);
                        if (!TestingHelper::compare($propValue1, $propValue2)) {
                            TestingHelper::logDebug(sprintf('compare: objects are different, property %s values: %s and %s',
                                $propName, TestingHelper::printValue($propValue1), TestingHelper::printValue($propValue2)));
                            return false;
                        }
                    }
                } else {
                    TestingHelper::logDebug(sprintf('compare: objects are different: %s property is absent', $propName));
                    return false;
                }
            }
            return true;
        }
    }
    
    private static function initValues(): void
    {
        TestingHelper::$primitiveValues = [
            ObjectType::BYTE => [
                'values' => [-128, 0, 127],
                'isMapKey' => true,
                'isArrayKey' => true
            ],
            ObjectType::SHORT => [
                'values' => [-32768, 0, 32767],
                'isMapKey' => true,
                'isArrayKey' => true
            ],
            ObjectType::INTEGER => [
                'values' => [12345, 0, -54321],
                'isMapKey' => true,
                'isArrayKey' => true,
                'typeOptional' => true
            ],
            ObjectType::LONG => [
                'values' => [12345678912345, 0, -98765432112345],
                'isMapKey' => false,
                'isArrayKey' => false,
            ],
            ObjectType::FLOAT => [
                'values' => [-1.155, 0.0, 123e-5],
                'isMapKey' => false,
                'isArrayKey' => false,
            ],
            ObjectType::DOUBLE => [
                'values' => [-123e5, 0.0, 0.0001],
                'isMapKey' => false,
                'isArrayKey' => false,
                'typeOptional' => true
            ],
            ObjectType::CHAR => [
                'values' => ['a', "\xC3\xA1"],
                'isMapKey' => true,
                'isArrayKey' => true,
            ],
            ObjectType::BOOLEAN => [
                'values' => [true, false],
                'isMapKey' => true,
                'isArrayKey' => false,
                'typeOptional' => true
            ],
            ObjectType::STRING => [
                'values' => ['abc', '', '0123'],
                'isMapKey' => true,
                'isArrayKey' => true,
                'typeOptional' => true
            ],
            ObjectType::UUID => [
                'values' => [
                    [ 18, 70, 2, 119, 154, 254, 198, 254, 195, 146, 33, 60, 116, 230, 0, 146 ],
                    [ 141, 77, 31, 194, 127, 36, 184, 255, 192, 4, 118, 57, 253, 209, 111, 147 ]
                ],
                'isMapKey' => false,
                'isArrayKey' => false
            ],
            ObjectType::DATE => [
                'values' => [
                    Date::fromDateTime(new DateTime('now')),
                    new Date(1234567890),
                    new Date(0)],
                'isMapKey' => false,
                'isArrayKey' => false,
                'typeOptional' => true
            ],
            ObjectType::DECIMAL => [
                'values' => [
                    BigDecimal::of('123456789.6789345'),
                    BigDecimal::of(0),
                    BigDecimal::of('-98765.4321e15')],
                'isMapKey' => false,
                'isArrayKey' => false,
                'typeOptional' => true
            ],
            ObjectType::TIME => [
                'values' => [
                    new Time(1234567),
                    new Time(123)],
                'isMapKey' => false,
                'isArrayKey' => false,
                'typeOptional' => true
            ],
            ObjectType::TIMESTAMP => [
                'values' => [
                    Timestamp::fromDateTime(new DateTime('now')),
                    new Timestamp(12345, 12345),
                    new Timestamp(0, 0)],
                'isMapKey' => false,
                'isArrayKey' => false,
                'typeOptional' => true
            ],
        ];
        
        TestingHelper::$arrayValues = [
            ObjectType::BYTE_ARRAY => [ 'elemType' => ObjectType::BYTE ],
            ObjectType::SHORT_ARRAY => [ 'elemType' => ObjectType::SHORT ],
            ObjectType::INTEGER_ARRAY => [ 'elemType' => ObjectType::INTEGER, 'typeOptional' => true ],
            ObjectType::LONG_ARRAY => [ 'elemType' => ObjectType::LONG ],
            ObjectType::FLOAT_ARRAY => [ 'elemType' => ObjectType::FLOAT ],
            ObjectType::DOUBLE_ARRAY => [ 'elemType' => ObjectType::DOUBLE, 'typeOptional' => true ],
            ObjectType::CHAR_ARRAY => [ 'elemType' => ObjectType::CHAR ],
            ObjectType::BOOLEAN_ARRAY => [ 'elemType' => ObjectType::BOOLEAN, 'typeOptional' => true ],
            ObjectType::STRING_ARRAY => [ 'elemType' => ObjectType::STRING, 'typeOptional' => true ],
            ObjectType::UUID_ARRAY => [ 'elemType' => ObjectType::UUID ],
            ObjectType::DATE_ARRAY => [ 'elemType' => ObjectType::DATE, 'typeOptional' => true ],
            ObjectType::DECIMAL_ARRAY => [ 'elemType' => ObjectType::DECIMAL, 'typeOptional' => true ],
            ObjectType::TIMESTAMP_ARRAY => [ 'elemType' => ObjectType::TIMESTAMP, 'typeOptional' => true ],
            ObjectType::TIME_ARRAY => [ 'elemType' => ObjectType::TIME, 'typeOptional' => true ],
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
    
    public static function printValue($value)
    {
        return print_r($value, true);
    }
    
    private static function logDebug($message): void
    {
        if (TestConfig::$debug) {
            echo($message . PHP_EOL);
        }
    }
}