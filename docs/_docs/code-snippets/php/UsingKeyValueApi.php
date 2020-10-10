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
require_once "/home/abudnikov/tmp/php-thin-client/vendor/autoload.php";

use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Cache\CacheEntry;
use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Query\ScanQuery;
use Apache\Ignite\Query\SqlFieldsQuery;

$client = new Client();
$clientConfiguration = new ClientConfiguration('127.0.0.1:10800');
// Connect to Ignite node
$client->connect($clientConfiguration);

//tag::createCache[]
$cacheCfg = new CacheConfiguration();
$cacheCfg->setCacheMode(CacheConfiguration::CACHE_MODE_REPLICATED);
$cacheCfg->setWriteSynchronizationMode(CacheConfiguration::WRITE_SYNC_MODE_FULL_SYNC);

$cache = $client->getOrCreateCache('References', $cacheCfg);
//end::createCache[]

//tag::basicOperations[]
$val = array();
$keys = range(1, 100);
foreach ($keys as $number) {
    $val[] = new CacheEntry($number, strval($number));
}
$cache->putAll($val);

$replace = $cache->replaceIfEquals(1, '2', '3');
echo $replace ? 'true' : 'false'; //false
echo "\r\n";

$value = $cache->get(1);
echo $value; //1
echo "\r\n";

$replace = $cache->replaceIfEquals(1, "1", 3);
echo $replace ? 'true' : 'false'; //true
echo "\r\n";

$value = $cache->get(1);
echo $value; //3
echo "\r\n";

$cache->put(101, '101');

$cache->removeKeys($keys);
$sizeIsOne = $cache->getSize() == 1;
echo $sizeIsOne ? 'true' : 'false'; //true
echo "\r\n";

$value = $cache->get(101);
echo $value; //101
echo "\r\n";

$cache->removeAll();
$sizeIsZero = $cache->getSize() == 0;
echo $sizeIsZero ? 'true' : 'false'; //true
echo "\r\n";

//end::basicOperations[]

class Person
{
    public $id;
    public $name;

    public function __construct($id, $name)
    {
        $this->id = $id;
        $this->name = $name;
    }

}

//tag::scanQry[]
$cache = $client->getOrCreateCache('personCache');

$cache->put(1, new Person(1, 'John Smith'));
$cache->put(1, new Person(1, 'John Johnson'));

$qry = new ScanQuery();
$cache->query(new ScanQuery());
//end::scanQry[]

$cache->removeAll();

//tag::executingSql[]
$create_table = new SqlFieldsQuery(
    sprintf('CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR) WITH "VALUE_TYPE=%s"', Person::class)
);
$create_table->setSchema('PUBLIC');
$cache->query($create_table)->getAll();

$key = 1;
$val = new Person(1, 'Person 1');

$insert = new SqlFieldsQuery('INSERT INTO Person(id, name) VALUES(?, ?)');
$insert->setArgs($val->id, $val->name);
$insert->setSchema('PUBLIC');
$cache->query($insert)->getAll();

$select = new SqlFieldsQuery('SELECT name FROM Person WHERE id = ?');
$select->setArgs($key);
$select->setSchema('PUBLIC');
$cursor = $cache->query($select);
// Get the results; the `getAll()` methods closes the cursor; you do not have to call cursor.close();
$results = $cursor->getAll();

if (sizeof($results) != 0) {
    echo 'name = ' . $results[0][0];
    echo "\r\n";
}

//end::executingSql[]
