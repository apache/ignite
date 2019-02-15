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

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Cache\CacheEntry;
use Apache\Ignite\Query\ScanQuery;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Data\BinaryObject;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\ComplexObjectType;

class Person
{
    private static $personId = 0;
    
    public $id;
    public $firstName;
    public $lastName;
    public $salary;
            
    public function __construct(string $firstName = null, string $lastName = null, float $salary = 0)
    {
        $this->id = Person::generateId();
        $this->firstName = $firstName;
        $this->lastName = $lastName;
        $this->salary = $salary;
    }

    public function printObject(): void
    {
        echo(sprintf('  %s%s', Person::class, PHP_EOL));
        Person::printField('id', $this->id);
        Person::printField('firstName', $this->firstName);
        Person::printField('lastName', $this->lastName);
        Person::printField('salary', $this->salary);
    }

    public static function generateId(): int
    {
        $id = Person::$personId;
        Person::$personId++;
        return $id;
    }
    
    public static function printField(string $fieldName, $fieldValue): void
    {
        echo(sprintf('      %s : %s%s', $fieldName, $fieldValue, PHP_EOL));
    }
}

// This example demonstrates basic Cache, Key-Value Queries and Scan Query operations:
// - connects to a node
// - creates a cache, if it doesn't exist
//   - specifies key type as Integer
// - executes different cache operations with Complex Objects and Binary Objects
//   - put several objects
//   - putAll
//   - get
//   - getAll
//   - ScanQuery
// - destroys the cache
class CachePutGetExample
{
    const ENDPOINT = '127.0.0.1:10800';
    const CACHE_NAME = 'CachePutGetExample_person';
    
    private $personCache;
    private $binaryObjectCache; 
    private $personObjectType;
    
    public function start(): void
    {
        $client = new Client();
        try {
            $client->connect(new ClientConfiguration(CachePutGetExample::ENDPOINT));

            $this->personObjectType = (new ComplexObjectType())->
                setFieldType('id', ObjectType::INTEGER);

            $this->personCache = $client->getOrCreateCache(CachePutGetExample::CACHE_NAME)->
                setKeyType(ObjectType::INTEGER)->
                setValueType($this->personObjectType);

            $this->binaryObjectCache = $client->getCache(CachePutGetExample::CACHE_NAME)->
                setKeyType(ObjectType::INTEGER);

            $this->putComplexObjects();
            $this->putAllBinaryObjects();

            $this->getAllComplexObjects();
            $this->getBinaryObjects();

            $this->scanQuery();

            $client->destroyCache(CachePutGetExample::CACHE_NAME);
        } catch (ClientException $e) {
            echo('ERROR: ' . $e->getMessage() . PHP_EOL);
        } finally {
            $client->disconnect();
        }
    }
    
    private function putComplexObjects(): void
    {
        $person1 = new Person('John', 'Doe', 1000);
        $person2 = new Person('Jane', 'Roe', 2000);

        $this->personCache->put($person1->id, $person1);
        $this->personCache->put($person2->id, $person2);

        echo('Complex Objects put successfully' . PHP_EOL);
    }
    
    private function putAllBinaryObjects(): void
    {
        // create binary object from scratch
        $personBinaryObject1 = (new BinaryObject(Person::class))->
            setField('id', Person::generateId(), ObjectType::INTEGER)->
            setField('firstName', 'Mary')->
            setField('lastName', 'Major')->
            setField('salary', 1500, ObjectType::DOUBLE);

        // create binary object from complex object
        $personBinaryObject2 = BinaryObject::fromObject(
            new Person('Richard', 'Miles', 800), $this->personObjectType);

        $this->binaryObjectCache->putAll([
            new CacheEntry($personBinaryObject1->getField('id'), $personBinaryObject1),
            new CacheEntry($personBinaryObject2->getField('id'), $personBinaryObject2)
        ]);
        
        echo('Binary Objects put successfully using putAll()' . PHP_EOL);
    }

    private function getAllComplexObjects(): void
    {
        $persons = $this->personCache->getAll([0, 1]);
        echo('Complex Objects getAll:' . PHP_EOL);
        foreach ($persons as $person) {
            $person->getValue()->printObject();
        }
    }
    
    private function getBinaryObjects(): void
    {
        $personBinaryObject = $this->binaryObjectCache->get(2);
        echo('Binary Object get:' . PHP_EOL);
        echo(sprintf("  %s%s", $personBinaryObject->getTypeName(), PHP_EOL));
        foreach ($personBinaryObject->getFieldNames() as $fieldName) {
            $fieldValue = $personBinaryObject->getField($fieldName);
            Person::printField($fieldName, $fieldValue); 
        }
    }

    private function scanQuery(): void
    {
        $cursor = $this->personCache->query(new ScanQuery());
        echo('Scan query results:' . PHP_EOL);
        foreach ($cursor as $cacheEntry) {
            $cacheEntry->getValue()->printObject();
        }
    }
}

$cachePutGetExample = new CachePutGetExample();
$cachePutGetExample->start();
