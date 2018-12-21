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

require_once __DIR__ . '/../vendor/autoload.php';

use Apache\Ignite\Client;
use Apache\Ignite\ClientConfiguration;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Cache\CacheInterface;
use Apache\Ignite\Cache\CacheConfiguration;
use Apache\Ignite\Query\SqlFieldsQuery;

// This example shows primary APIs to use with Ignite as with an SQL database:
// - connects to a node
// - creates a cache, if it doesn't exist
// - creates tables (CREATE TABLE)
// - creates indices (CREATE INDEX)
// - writes data of primitive types into the tables (INSERT INTO table)
// - reads data from the tables (SELECT ...)
// - deletes tables (DROP TABLE)
// - destroys the cache
class SqlExample {
    const ENDPOINT = '127.0.0.1:10800';

    const COUNTRY_CACHE_NAME = 'Country';
    const CITY_CACHE_NAME = 'City';
    const COUNTRY_LANGUAGE_CACHE_NAME = 'CountryLng';
    const DUMMY_CACHE_NAME = 'SqlExample_Dummy';

    public function start(): void
    {
        $client = new Client();
        try {
            $client->connect(new ClientConfiguration(SqlExample::ENDPOINT));

            $cache = $client->getOrCreateCache(
                SqlExample::DUMMY_CACHE_NAME,
                (new CacheConfiguration())->setSqlSchema('PUBLIC'));

            $this->createDatabaseObjects($cache);
            $this->insertData($cache);

            $countryCache = $client->getCache(SqlExample::COUNTRY_CACHE_NAME);
            $cityCache = $client->getCache(SqlExample::CITY_CACHE_NAME);

            $this->getMostPopulatedCities($countryCache);
            $this->getTopCitiesInThreeCountries($cityCache);
            $this->getCityDetails($cityCache, 5);

            $this->deleteDatabaseObjects($cache);
            $client->destroyCache(SqlExample::DUMMY_CACHE_NAME);
        } catch (ClientException $e) {
            echo('ERROR: ' . $e->getMessage() . PHP_EOL);
        } finally {
            $client->disconnect();
        }
    }

    public function createDatabaseObjects(CacheInterface $cache): void
    {
        $createCountryTable = 'CREATE TABLE Country (' .
            'Code CHAR(3) PRIMARY KEY,' .
            'Name CHAR(52),' .
            'Continent CHAR(50),' .
            'Region CHAR(26),' .
            'SurfaceArea DECIMAL(10,2),' .
            'IndepYear SMALLINT(6),' .
            'Population INT(11),' .
            'LifeExpectancy DECIMAL(3,1),' .
            'GNP DECIMAL(10,2),' .
            'GNPOld DECIMAL(10,2),' .
            'LocalName CHAR(45),' .
            'GovernmentForm CHAR(45),' .
            'HeadOfState CHAR(60),' .
            'Capital INT(11),' .
            'Code2 CHAR(2)' .
        ') WITH "template=partitioned, backups=1, CACHE_NAME=' . SqlExample::COUNTRY_CACHE_NAME . '"';

        $createCityTable = 'CREATE TABLE City (' .
            'ID INT(11),' .
            'Name CHAR(35),' .
            'CountryCode CHAR(3),' .
            'District CHAR(20),' .
            'Population INT(11),' .
            'PRIMARY KEY (ID, CountryCode)' .
        ') WITH "template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=' . SqlExample::CITY_CACHE_NAME . '"';

        $createCountryLanguageTable = 'CREATE TABLE CountryLanguage (' .
            'CountryCode CHAR(3),' .
            'Language CHAR(30),' .
            'IsOfficial CHAR(2),' .
            'Percentage DECIMAL(4,1),' .
            'PRIMARY KEY (CountryCode, Language)' .
        ') WITH "template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=' . SqlExample::COUNTRY_LANGUAGE_CACHE_NAME . '"';

        // create tables
        $cache->query(new SqlFieldsQuery($createCountryTable))->getAll();
        $cache->query(new SqlFieldsQuery($createCityTable))->getAll();
        $cache->query(new SqlFieldsQuery($createCountryLanguageTable))->getAll();

        // create indices
        $cache->query(new SqlFieldsQuery(
            'CREATE INDEX idx_country_code ON city (CountryCode)'))->getAll();
        $cache->query(new SqlFieldsQuery(
            'CREATE INDEX idx_lang_country_code ON CountryLanguage (CountryCode)'))->getAll();

        echo('Database objects created' . PHP_EOL);
    }

    private function insertData(CacheInterface $cache): void
    {
        $cities = [
            ['New York', 'USA', 'New York', 8008278],
            ['Los Angeles', 'USA', 'California', 3694820],
            ['Chicago', 'USA', 'Illinois', 2896016],
            ['Houston', 'USA', 'Texas', 1953631],
            ['Philadelphia', 'USA', 'Pennsylvania', 1517550],
            ['Moscow', 'RUS', 'Moscow (City)', 8389200],
            ['St Petersburg', 'RUS', 'Pietari', 4694000],
            ['Novosibirsk', 'RUS', 'Novosibirsk', 1398800],
            ['Nizni Novgorod', 'RUS', 'Nizni Novgorod', 1357000],
            ['Jekaterinburg', 'RUS', 'Sverdlovsk', 1266300],
            ['Shanghai', 'CHN', 'Shanghai', 9696300],
            ['Peking', 'CHN', 'Peking', 7472000],
            ['Chongqing', 'CHN', 'Chongqing', 6351600],
            ['Tianjin', 'CHN', 'Tianjin', 5286800],
            ['Wuhan', 'CHN', 'Hubei', 4344600]
        ];

        $cityQuery = new SqlFieldsQuery(
            'INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (?, ?, ?, ?, ?)');

        for ($i = 0; $i < count($cities); $i++) {
            $cache->query($cityQuery->setArgs($i, ...$cities[$i]))->getAll();
        }

        $countries = [
            ['USA', 'United States', 'North America', 'North America',
                9363520.00, 1776, 278357000, 77.1, 8510700.00, 8110900.00,
                'United States', 'Federal Republic', 'George W. Bush', 3813, 'US'],
            ['RUS', 'Russian Federation', 'Europe', 'Eastern Europe',
                17075400.00, 1991, 146934000, 67.2, 276608.00, 442989.00,
                'Rossija', 'Federal Republic', 'Vladimir Putin', 3580, 'RU'],
            ['CHN', 'China', 'Asia', 'Eastern Asia',
                9572900.00, -1523, 1277558000, 71.4, 982268.00, 917719.00,
                'Zhongquo', 'PeoplesRepublic', 'Jiang Zemin', 1891, 'CN']
        ];

        $countryQuery = new SqlFieldsQuery('INSERT INTO Country(' .
            'Code, Name, Continent, Region, SurfaceArea,' .
            'IndepYear, Population, LifeExpectancy, GNP, GNPOld,' .
            'LocalName, GovernmentForm, HeadOfState, Capital, Code2)' .
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');

        foreach ($countries as $country) {
            $cache->query($countryQuery->setArgs(...$country))->getAll();
        }

        echo('Data are inserted' . PHP_EOL);
    }

    private function getMostPopulatedCities(CacheInterface $countryCache): void
    {
        $query = new SqlFieldsQuery(
            'SELECT name, population FROM City ORDER BY population DESC LIMIT 10');

        $cursor = $countryCache->query($query);

        echo('10 Most Populated Cities:' . PHP_EOL);

        foreach ($cursor as $row) {
            echo(sprintf('    %d people live in %s%s', $row[1], $row[0], PHP_EOL));
        }
    }

    private function getTopCitiesInThreeCountries(CacheInterface $countryCache): void
    {
        $query = new SqlFieldsQuery(
            "SELECT country.name, city.name, MAX(city.population) as max_pop FROM country
            JOIN city ON city.countrycode = country.code
            WHERE country.code IN ('USA','RUS','CHN')
            GROUP BY country.name, city.name ORDER BY max_pop DESC LIMIT 3");

        $cursor = $countryCache->query($query);

        echo('3 Most Populated Cities in US, RUS and CHN:' . PHP_EOL);

        foreach ($cursor->getAll() as $row) {
            echo(sprintf('    %d people live in %s, %s%s', $row[2], $row[1], $row[0], PHP_EOL));
        }
    }

    private function getCityDetails(CacheInterface $cityCache, int $cityId): void
    {
        $query = (new SqlFieldsQuery('SELECT * FROM City WHERE id = ?'))->
            setArgs($cityId)->
            setIncludeFieldNames(true);

        $cursor = $cityCache->query($query);

        $fieldNames = $cursor->getFieldNames();
        
        foreach ($cursor->getAll() as $city) {
            echo('City Info:' . PHP_EOL);
            for ($i = 0; $i < count($fieldNames); $i++) {
                echo(sprintf('    %s : %s%s', $fieldNames[$i], $city[$i], PHP_EOL));
            }
        }
    }

    private function deleteDatabaseObjects($cache): void
    {
        $cache->query(new SqlFieldsQuery('DROP TABLE IF EXISTS Country'))->getAll();
        $cache->query(new SqlFieldsQuery('DROP TABLE IF EXISTS City'))->getAll();
        $cache->query(new SqlFieldsQuery('DROP TABLE IF EXISTS CountryLanguage'))->getAll();
        
        echo('Database objects dropped' . PHP_EOL);
    }
}

$sqlExample = new SqlExample();
$sqlExample->start();
