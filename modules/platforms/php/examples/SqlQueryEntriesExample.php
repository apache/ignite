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
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Cache\QueryEntity;
use Apache\Ignite\Cache\QueryField;
use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Query\SqlQuery;

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

    public static function generateId(): int
    {
        $id = Person::$personId;
        Person::$personId++;
        return $id;
    }
}

// This example demonstrates basic Cache, Key-Value Queries and SQL Query operations:
// - connects to a node
// - creates a cache from CacheConfiguration, if it doesn't exist
// - writes data of primitive and Complex Object types into the cache using Key-Value put operation
// - reads data from the cache using SQL Query
// - destroys the cache
class SqlQueryEntriesExample {
    const ENDPOINT = '127.0.0.1:10800';
    const PERSON_CACHE_NAME = 'SqlQueryEntriesExample_person';

    private $cache;

    public function start(): void
    {
        $client = new Client();
        try {
            $client->connect(new ClientConfiguration(self::ENDPOINT));

            $cacheCfg = (new CacheConfiguration())->
                setQueryEntities(
                    (new QueryEntity())->
                    setValueTypeName('Person')->
                    setFields(
                        new QueryField('id', 'java.lang.Integer'),
                        new QueryField('firstName', 'java.lang.String'),
                        new QueryField('lastName', 'java.lang.String'),
                        new QueryField('salary', 'java.lang.Double')
                    ));
            $this->cache = $client->getOrCreateCache(self::PERSON_CACHE_NAME, $cacheCfg)->
                setKeyType(ObjectType::INTEGER)->
                setValueType((new ComplexObjectType())->
                    setFieldType('id', ObjectType::INTEGER));

            $this->generateData();

            $sqlCursor = $this->cache->query(
                (new SqlQuery('Person', 'salary > ? and salary <= ?'))->
                setArgs(900.0, 1600.0));

            echo('SqlQuery results (salary between 900 and 1600):' . PHP_EOL);
            foreach ($sqlCursor as $cacheEntry) {
                $person = $cacheEntry->getValue();
                echo(sprintf('  name: %s %s, salary: %.2f %s',
                    $person->firstName, $person->lastName, $person->salary, PHP_EOL));
            }

            $client->destroyCache(self::PERSON_CACHE_NAME);
        } catch (ClientException $e) {
            echo('ERROR: ' . $e->getMessage() . PHP_EOL);
        } finally {
            $client->disconnect();
        }
    }

    private function generateData(): void
    {
        $persons = [
            ['John', 'Doe', 1000.0],
            ['Jane', 'Roe', 2000.0],
            ['Mary', 'Major', 1500.0],
            ['Richard', 'Miles', 800.0]
        ];
        foreach ($persons as $data) {
            $person = new Person(...$data);
            $this->cache->put($person->id, $person);
        }
        echo('Data is generated' . PHP_EOL);
    }
}

$sqlQueryEntriesExample = new SqlQueryEntriesExample();
$sqlQueryEntriesExample->start();
