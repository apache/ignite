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

namespace Apache\Ignite\Cache;

use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Type\ObjectArrayType;
use Apache\Ignite\Type\ComplexObjectType;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\BinaryUtils;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Utils\ArgumentChecker;

/**
 * Class representing Ignite cache configuration on a server.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class CacheConfiguration
{
    const PROP_NAME = 0;
    const PROP_CACHE_MODE = 1;
    const PROP_ATOMICITY_MODE = 2;
    const PROP_BACKUPS = 3;
    const PROP_WRITE_SYNCHRONIZATION_MODE = 4;
    const PROP_COPY_ON_READ = 5;
    const PROP_READ_FROM_BACKUP = 6;
    const PROP_DATA_REGION_NAME = 100;
    const PROP_IS_ONHEAP_CACHE_ENABLED = 101;
    const PROP_QUERY_ENTITY = 200;
    const PROP_QUERY_PARALLELISM = 201;
    const PROP_QUERY_DETAIL_METRICS_SIZE = 202;
    const PROP_SQL_SCHEMA = 203;
    const PROP_SQL_INDEX_INLINE_MAX_SIZE = 204;
    const PROP_SQL_ESCAPE_ALL = 205;
    const PROP_MAX_QUERY_ITERATORS = 206;
    const PROP_REBALANCE_MODE = 300;
    const PROP_REBALANCE_DELAY = 301;
    const PROP_REBALANCE_TIMEOUT = 302;
    const PROP_REBALANCE_BATCH_SIZE = 303;
    const PROP_REBALANCE_BATCHES_PREFETCH_COUNT = 304;
    const PROP_REBALANCE_ORDER = 305;
    const PROP_REBALANCE_THROTTLE = 306;
    const PROP_GROUP_NAME = 400;
    const PROP_CACHE_KEY_CONFIGURATION = 401;
    const PROP_DEFAULT_LOCK_TIMEOUT = 402;
    const PROP_MAX_CONCURRENT_ASYNC_OPS = 403;
    const PROP_PARTITION_LOSS_POLICY = 404;
    const PROP_EAGER_TTL = 405;
    const PROP_STATISTICS_ENABLED = 406;

    /** @name AtomicityMode
     *  @anchor AtomicityMode
     *  @{
     */
    const ATOMICITY_MODE_TRANSACTIONAL = 0;
    const ATOMICITY_MODE_ATOMIC = 1;
    /** @} */ // end of AtomicityMode

    /** @name CacheMode
     *  @anchor CacheMode
     *  @{
     */
    const CACHE_MODE_LOCAL = 0;
    const CACHE_MODE_REPLICATED = 1;
    const CACHE_MODE_PARTITIONED = 2;
    /** @} */ // end of CacheMode

    /** @name PartitionLossPolicy
     *  @anchor PartitionLossPolicy
     *  @{
     */
    const PARTITION_LOSS_POLICY_READ_ONLY_SAFE = 0;
    const PARTITION_LOSS_POLICY_READ_ONLY_ALL = 1;
    const PARTITION_LOSS_POLICY_READ_WRITE_SAFE = 2;
    const PARTITION_LOSS_POLICY_READ_WRITE_ALL = 3;
    const PARTITION_LOSS_POLICY_IGNORE = 4;
    /** @} */ // end of PartitionLossPolicy

    /** @name RebalanceMode
     *  @anchor RebalanceMode
     *  @{
     */
    const REABALANCE_MODE_SYNC = 0;
    const REABALANCE_MODE_ASYNC = 1;
    const REABALANCE_MODE_NONE = 2;
    /** @} */ // end of RebalanceMode

    /** @name WriteSynchronizationMode
     *  @anchor WriteSynchronizationMode
     *  @{
     */
    const WRITE_SYNC_MODE_FULL_SYNC = 0;
    const WRITE_SYNC_MODE_FULL_ASYNC = 1;
    const WRITE_SYNC_MODE_PRIMARY_SYNC = 2;
    /** @} */ // end of WriteSynchronizationMode

    private $properties;
    private static $propInfo;

    /**
     * Public CacheConfiguration constructor.
     */
    public function __construct()
    {
        $this->properties = [];
    }

    /**
     *
     *
     * @param string $name
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setName(string $name): CacheConfiguration
    {
        $this->properties[self::PROP_NAME] = $name;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getName(): ?string
    {
        return $this->getProperty(self::PROP_NAME);
    }

    /**
     *
     *
     * @param int $atomicityMode one of @ref AtomicityMode constants.
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     *
     * @throws ClientException if error.
     */
    public function setAtomicityMode(int $atomicityMode): CacheConfiguration
    {
        ArgumentChecker::hasValueFrom(
            $atomicityMode, 'atomicityMode', false, [self::ATOMICITY_MODE_TRANSACTIONAL, self::ATOMICITY_MODE_ATOMIC]);
        $this->properties[self::PROP_ATOMICITY_MODE] = $atomicityMode;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getAtomicityMode(): ?int
    {
        return $this->getProperty(self::PROP_ATOMICITY_MODE);
    }

    /**
     *
     *
     * @param int $backups
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setBackups(int $backups): CacheConfiguration
    {
        $this->properties[self::PROP_BACKUPS] = $backups;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getBackups(): ?int
    {
        return $this->getProperty(self::PROP_BACKUPS);
    }

    /**
     *
     *
     * @param int $cacheMode one of @ref CacheMode constants.
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     *
     * @throws ClientException if error.
     */
    public function setCacheMode(int $cacheMode): CacheConfiguration
    {
        ArgumentChecker::hasValueFrom(
            $cacheMode,
            'cacheMode',
            false,
            [self::CACHE_MODE_LOCAL, self::CACHE_MODE_REPLICATED, self::CACHE_MODE_PARTITIONED]);
        $this->properties[self::PROP_CACHE_MODE] = $cacheMode;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getCacheMode(): ?int
    {
        return $this->getProperty(self::PROP_CACHE_MODE);
    }

    /**
     *
     *
     * @param bool $copyOnRead
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setCopyOnRead(bool $copyOnRead): CacheConfiguration
    {
        $this->properties[self::PROP_COPY_ON_READ] = $copyOnRead;
        return $this;
    }

    /**
     *
     *
     * @return bool|null
     */
    public function getCopyOnRead(): ?bool
    {
        return $this->getProperty(self::PROP_COPY_ON_READ);
    }

    /**
     *
     *
     * @param string|null $dataRegionName
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setDataRegionName(?string $dataRegionName): CacheConfiguration
    {
        $this->properties[self::PROP_DATA_REGION_NAME] = $dataRegionName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getDataRegionName(): ?string
    {
        return $this->getProperty(self::PROP_DATA_REGION_NAME);
    }

    /**
     *
     *
     * @param bool $eagerTtl
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setEagerTtl(bool $eagerTtl): CacheConfiguration
    {
        $this->properties[self::PROP_EAGER_TTL] = $eagerTtl;
        return $this;
    }

    /**
     *
     *
     * @return bool|null
     */
    public function getEagerTtl(): ?bool
    {
        return $this->getProperty(self::PROP_EAGER_TTL);
    }

    /**
     *
     *
     * @param bool $statisticsEnabled
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setStatisticsEnabled(bool $statisticsEnabled): CacheConfiguration
    {
        $this->properties[self::PROP_STATISTICS_ENABLED] = $statisticsEnabled;
        return $this;
    }

    /**
     *
     *
     * @return bool|null
     */
    public function getStatisticsEnabled(): ?bool
    {
        return $this->getProperty(self::PROP_STATISTICS_ENABLED);
    }

    /**
     *
     *
     * @param string|null $groupName
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setGroupName(?string $groupName): CacheConfiguration
    {
        $this->properties[self::PROP_GROUP_NAME] = $groupName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getGroupName(): ?string
    {
        return $this->getProperty(self::PROP_GROUP_NAME);
    }

    /**
     *
     *
     * @param float $lockTimeout
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setDefaultLockTimeout(float $lockTimeout): CacheConfiguration
    {
        $this->properties[self::PROP_DEFAULT_LOCK_TIMEOUT] = $lockTimeout;
        return $this;
    }

    /**
     *
     *
     * @return float|null
     */
    public function getDefaultLockTimeout(): ?float
    {
        return $this->getProperty(self::PROP_DEFAULT_LOCK_TIMEOUT);
    }

    /**
     *
     *
     * @param int $maxConcurrentAsyncOperations
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setMaxConcurrentAsyncOperations(int $maxConcurrentAsyncOperations): CacheConfiguration
    {
        $this->properties[self::PROP_MAX_CONCURRENT_ASYNC_OPS] = $maxConcurrentAsyncOperations;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getMaxConcurrentAsyncOperations(): ?int
    {
        return $this->getProperty(self::PROP_MAX_CONCURRENT_ASYNC_OPS);
    }

    /**
     *
     *
     * @param int $maxQueryIterators
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setMaxQueryIterators(int $maxQueryIterators): CacheConfiguration
    {
        $this->properties[self::PROP_MAX_QUERY_ITERATORS] = $maxQueryIterators;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getMaxQueryIterators(): ?int
    {
        return $this->getProperty(self::PROP_MAX_QUERY_ITERATORS);
    }

    /**
     *
     *
     * @param bool $isOnheapCacheEnabled
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setIsOnheapCacheEnabled(bool $isOnheapCacheEnabled): CacheConfiguration
    {
        $this->properties[self::PROP_IS_ONHEAP_CACHE_ENABLED] = $isOnheapCacheEnabled;
        return $this;
    }

    /**
     *
     *
     * @return bool|null
     */
    public function getIsOnheapCacheEnabled(): ?bool
    {
        return $this->getProperty(self::PROP_IS_ONHEAP_CACHE_ENABLED);
    }

    /**
     *
     *
     * @param int $partitionLossPolicy one of @ref PartitionLossPolicy constants.
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     *
     * @throws ClientException if error.
     */
    public function setPartitionLossPolicy(int $partitionLossPolicy): CacheConfiguration
    {
        ArgumentChecker::hasValueFrom(
            $partitionLossPolicy,
            'partitionLossPolicy',
            false,
            [
                self::PARTITION_LOSS_POLICY_READ_ONLY_SAFE,
                self::PARTITION_LOSS_POLICY_READ_ONLY_ALL,
                self::PARTITION_LOSS_POLICY_READ_WRITE_SAFE,
                self::PARTITION_LOSS_POLICY_READ_WRITE_ALL,
                self::PARTITION_LOSS_POLICY_IGNORE
            ]);
        $this->properties[self::PROP_PARTITION_LOSS_POLICY] = $partitionLossPolicy;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getPartitionLossPolicy(): ?int
    {
        return $this->getProperty(self::PROP_PARTITION_LOSS_POLICY);
    }

    /**
     *
     *
     * @param int $queryDetailMetricsSize
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setQueryDetailMetricsSize(int $queryDetailMetricsSize): CacheConfiguration
    {
        $this->properties[self::PROP_QUERY_DETAIL_METRICS_SIZE] = $queryDetailMetricsSize;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getQueryDetailMetricsSize(): ?int
    {
        return $this->getProperty(self::PROP_QUERY_DETAIL_METRICS_SIZE);
    }

    /**
     *
     *
     * @param int $queryParallelism
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setQueryParallelism(int $queryParallelism): CacheConfiguration
    {
        $this->properties[self::PROP_QUERY_PARALLELISM] = $queryParallelism;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getQueryParallelism(): ?int
    {
        return $this->getProperty(self::PROP_QUERY_PARALLELISM);
    }

    /**
     *
     *
     * @param bool $readFromBackup
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setReadFromBackup(bool $readFromBackup): CacheConfiguration
    {
        $this->properties[self::PROP_READ_FROM_BACKUP] = $readFromBackup;
        return $this;
    }

    /**
     *
     *
     * @return bool|null
     */
    public function getReadFromBackup(): ?bool
    {
        return $this->getProperty(self::PROP_READ_FROM_BACKUP);
    }

    /**
     *
     *
     * @param int $rebalanceBatchSize
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setRebalanceBatchSize(int $rebalanceBatchSize): CacheConfiguration
    {
        $this->properties[self::PROP_REBALANCE_BATCH_SIZE] = $rebalanceBatchSize;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getRebalanceBatchSize(): ?int
    {
        return $this->getProperty(self::PROP_REBALANCE_BATCH_SIZE);
    }

    /**
     *
     *
     * @param float $rebalanceBatchesPrefetchCount
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setRebalanceBatchesPrefetchCount(float $rebalanceBatchesPrefetchCount): CacheConfiguration
    {
        $this->properties[self::PROP_REBALANCE_BATCHES_PREFETCH_COUNT] = $rebalanceBatchesPrefetchCount;
        return $this;
    }

    /**
     *
     *
     * @return float|null
     */
    public function getRebalanceBatchesPrefetchCount(): ?float
    {
        return $this->getProperty(self::PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
    }

    /**
     *
     *
     * @param float $rebalanceDelay
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setRebalanceDelay(float $rebalanceDelay): CacheConfiguration
    {
        $this->properties[self::PROP_REBALANCE_DELAY] = $rebalanceDelay;
        return $this;
    }

    /**
     *
     *
     * @return float|null
     */
    public function getRebalanceDelay(): ?float
    {
        return $this->getProperty(self::PROP_REBALANCE_DELAY);
    }

    /**
     *
     *
     * @param int $rebalanceMode one of @ref RebalanceMode constants.
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     *
     * @throws ClientException if error.
     */
    public function setRebalanceMode(int $rebalanceMode): CacheConfiguration
    {
        ArgumentChecker::hasValueFrom(
            $rebalanceMode,
            'rebalanceMode',
            false,
            [self::REABALANCE_MODE_SYNC, self::REABALANCE_MODE_ASYNC, self::REABALANCE_MODE_NONE]);
        $this->properties[self::PROP_REBALANCE_MODE] = $rebalanceMode;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getRebalanceMode(): ?int
    {
        return $this->getProperty(self::PROP_REBALANCE_MODE);
    }

    /**
     *
     *
     * @param int $rebalanceOrder
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setRebalanceOrder(int $rebalanceOrder): CacheConfiguration
    {
        $this->properties[self::PROP_REBALANCE_ORDER] = $rebalanceOrder;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getRebalanceOrder(): ?int
    {
        return $this->getProperty(self::PROP_REBALANCE_ORDER);
    }

    /**
     *
     *
     * @param float $rebalanceThrottle
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setRebalanceThrottle(float $rebalanceThrottle): CacheConfiguration
    {
        $this->properties[self::PROP_REBALANCE_THROTTLE] = $rebalanceThrottle;
        return $this;
    }

    /**
     *
     *
     * @return float|null
     */
    public function getRebalanceThrottle(): ?float
    {
        return $this->getProperty(self::PROP_REBALANCE_THROTTLE);
    }

    /**
     *
     *
     * @param float $rebalanceTimeout
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setRebalanceTimeout(float $rebalanceTimeout): CacheConfiguration
    {
        $this->properties[self::PROP_REBALANCE_TIMEOUT] = $rebalanceTimeout;
        return $this;
    }

    /**
     *
     *
     * @return float|null
     */
    public function getRebalanceTimeout(): ?float
    {
        return $this->getProperty(self::PROP_REBALANCE_TIMEOUT);
    }

    /**
     *
     *
     * @param bool $sqlEscapeAll
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setSqlEscapeAll(bool $sqlEscapeAll): CacheConfiguration
    {
        $this->properties[self::PROP_SQL_ESCAPE_ALL] = $sqlEscapeAll;
        return $this;
    }

    /**
     *
     *
     * @return bool|null
     */
    public function getSqlEscapeAll(): ?bool
    {
        return $this->getProperty(self::PROP_SQL_ESCAPE_ALL);
    }

    /**
     *
     *
     * @param int $sqlIndexInlineMaxSize
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setSqlIndexInlineMaxSize(int $sqlIndexInlineMaxSize): CacheConfiguration
    {
        $this->properties[self::PROP_SQL_INDEX_INLINE_MAX_SIZE] = $sqlIndexInlineMaxSize;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getSqlIndexInlineMaxSize(): ?int
    {
        return $this->getProperty(self::PROP_SQL_INDEX_INLINE_MAX_SIZE);
    }

    /**
     *
     *
     * @param string|null $sqlSchema
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setSqlSchema(?string $sqlSchema): CacheConfiguration
    {
        $this->properties[self::PROP_SQL_SCHEMA] = $sqlSchema;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getSqlSchema(): ?string
    {
        return $this->getProperty(self::PROP_SQL_SCHEMA);
    }

    /**
     *
     *
     * @param int $writeSynchronizationMode one of @ref WriteSynchronizationMode constants.
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     *
     * @throws ClientException if error.
     */
    public function setWriteSynchronizationMode(int $writeSynchronizationMode): CacheConfiguration
    {
        ArgumentChecker::hasValueFrom(
            $writeSynchronizationMode,
            'writeSynchronizationMode',
            false,
            [self::WRITE_SYNC_MODE_FULL_SYNC, self::WRITE_SYNC_MODE_FULL_ASYNC, self::WRITE_SYNC_MODE_PRIMARY_SYNC]);
        $this->properties[self::PROP_WRITE_SYNCHRONIZATION_MODE] = $writeSynchronizationMode;
        return $this;
    }

    /**
     *
     *
     * @return int|null
     */
    public function getWriteSynchronizationMode(): ?int
    {
        return $this->getProperty(self::PROP_WRITE_SYNCHRONIZATION_MODE);
    }

    /**
     *
     *
     * @param CacheKeyConfiguration ...$keyConfigurations
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setKeyConfigurations(CacheKeyConfiguration ...$keyConfigurations): CacheConfiguration
    {
        $this->properties[self::PROP_CACHE_KEY_CONFIGURATION] = $keyConfigurations;
        return $this;
    }

    /**
     *
     *
     * @return CacheKeyConfiguration[]|null
     */
    public function getKeyConfigurations(): ?array
    {
        return $this->getProperty(self::PROP_CACHE_KEY_CONFIGURATION);
    }

    /**
     *
     *
     * @param QueryEntity ...$queryEntities
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setQueryEntities(QueryEntity ...$queryEntities): CacheConfiguration
    {
        $this->properties[self::PROP_QUERY_ENTITY] = $queryEntities;
        return $this;
    }

    /**
     *
     *
     * @return QueryEntity[]|null
     */
    public function getQueryEntities(): ?array
    {
        return $this->getProperty(self::PROP_QUERY_ENTITY);
    }

    private function getProperty(int $prop)
    {
        if (array_key_exists($prop, $this->properties)) {
            return $this->properties[$prop];
        }
        return null;
    }

    static function init(): void
    {
        self::$propInfo = array(
            self::PROP_NAME => ObjectType::STRING,
            self::PROP_CACHE_MODE => ObjectType::INTEGER,
            self::PROP_ATOMICITY_MODE => ObjectType::INTEGER,
            self::PROP_BACKUPS => ObjectType::INTEGER,
            self::PROP_WRITE_SYNCHRONIZATION_MODE => ObjectType::INTEGER,
            self::PROP_COPY_ON_READ => ObjectType::BOOLEAN,
            self::PROP_READ_FROM_BACKUP => ObjectType::BOOLEAN,
            self::PROP_DATA_REGION_NAME => ObjectType::STRING,
            self::PROP_IS_ONHEAP_CACHE_ENABLED => ObjectType::BOOLEAN,
            self::PROP_QUERY_ENTITY => new ObjectArrayType((new ComplexObjectType())->setPhpClassName(QueryEntity::class)),
            self::PROP_QUERY_PARALLELISM => ObjectType::INTEGER,
            self::PROP_QUERY_DETAIL_METRICS_SIZE => ObjectType::INTEGER,
            self::PROP_SQL_SCHEMA => ObjectType::STRING,
            self::PROP_SQL_INDEX_INLINE_MAX_SIZE => ObjectType::INTEGER,
            self::PROP_SQL_ESCAPE_ALL => ObjectType::BOOLEAN,
            self::PROP_MAX_QUERY_ITERATORS => ObjectType::INTEGER,
            self::PROP_REBALANCE_MODE => ObjectType::INTEGER,
            self::PROP_REBALANCE_DELAY => ObjectType::LONG,
            self::PROP_REBALANCE_TIMEOUT => ObjectType::LONG,
            self::PROP_REBALANCE_BATCH_SIZE => ObjectType::INTEGER,
            self::PROP_REBALANCE_BATCHES_PREFETCH_COUNT => ObjectType::LONG,
            self::PROP_REBALANCE_ORDER => ObjectType::INTEGER,
            self::PROP_REBALANCE_THROTTLE => ObjectType::LONG,
            self::PROP_GROUP_NAME => ObjectType::STRING,
            self::PROP_CACHE_KEY_CONFIGURATION => new ObjectArrayType((new ComplexObjectType())->setPhpClassName(CacheKeyConfiguration::class)),
            self::PROP_DEFAULT_LOCK_TIMEOUT => ObjectType::LONG,
            self::PROP_MAX_CONCURRENT_ASYNC_OPS => ObjectType::INTEGER,
            self::PROP_PARTITION_LOSS_POLICY => ObjectType::INTEGER,
            self::PROP_EAGER_TTL => ObjectType::BOOLEAN,
            self::PROP_STATISTICS_ENABLED => ObjectType::BOOLEAN
        );
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer, string $name): void
    {
        $this->setName($name);

        $startPos = $buffer->getPosition();
        $buffer->setPosition($startPos +
            BinaryUtils::getSize(ObjectType::INTEGER) +
            BinaryUtils::getSize(ObjectType::SHORT));

        foreach ($this->properties as $propertyCode => $property) {
            $this->writeProperty($communicator, $buffer, $propertyCode, $property);
        }

        $length = $buffer->getPosition() - $startPos;
        $buffer->setPosition($startPos);

        $buffer->writeInteger($length);
        $buffer->writeShort(count($this->properties));
    }

    private function writeProperty(BinaryCommunicator $communicator, MessageBuffer $buffer, int $propertyCode, $property): void
    {
        $buffer->writeShort($propertyCode);
        $propertyType = self::$propInfo[$propertyCode];
        switch (BinaryUtils::getTypeCode($propertyType)) {
            case ObjectType::INTEGER:
            case ObjectType::LONG:
            case ObjectType::BOOLEAN:
                $communicator->writeObject($buffer, $property, $propertyType, false);
                return;
            case ObjectType::STRING:
                $communicator->writeObject($buffer, $property, $propertyType);
                return;
            case ObjectType::OBJECT_ARRAY:
                $length = $property ? count($property) : 0;
                $buffer->writeInteger($length);
                foreach ($property as $prop) {
                    $prop->write($communicator, $buffer);
                }
                return;
            default:
                BinaryUtils::internalError();
        }
    }

    // This is not the public API method, is not intended for usage by an application.
    public function read(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        //length
        $buffer->readInteger();
        $this->readProperty($communicator, $buffer, self::PROP_ATOMICITY_MODE);
        $this->readProperty($communicator, $buffer, self::PROP_BACKUPS);
        $this->readProperty($communicator, $buffer, self::PROP_CACHE_MODE);
        $this->readProperty($communicator, $buffer, self::PROP_COPY_ON_READ);
        $this->readProperty($communicator, $buffer, self::PROP_DATA_REGION_NAME);
        $this->readProperty($communicator, $buffer, self::PROP_EAGER_TTL);
        $this->readProperty($communicator, $buffer, self::PROP_STATISTICS_ENABLED);
        $this->readProperty($communicator, $buffer, self::PROP_GROUP_NAME);
        $this->readProperty($communicator, $buffer, self::PROP_DEFAULT_LOCK_TIMEOUT);
        $this->readProperty($communicator, $buffer, self::PROP_MAX_CONCURRENT_ASYNC_OPS);
        $this->readProperty($communicator, $buffer, self::PROP_MAX_QUERY_ITERATORS);
        $this->readProperty($communicator, $buffer, self::PROP_NAME);
        $this->readProperty($communicator, $buffer, self::PROP_IS_ONHEAP_CACHE_ENABLED);
        $this->readProperty($communicator, $buffer, self::PROP_PARTITION_LOSS_POLICY);
        $this->readProperty($communicator, $buffer, self::PROP_QUERY_DETAIL_METRICS_SIZE);
        $this->readProperty($communicator, $buffer, self::PROP_QUERY_PARALLELISM);
        $this->readProperty($communicator, $buffer, self::PROP_READ_FROM_BACKUP);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_BATCH_SIZE);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_BATCHES_PREFETCH_COUNT);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_DELAY);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_MODE);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_ORDER);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_THROTTLE);
        $this->readProperty($communicator, $buffer, self::PROP_REBALANCE_TIMEOUT);
        $this->readProperty($communicator, $buffer, self::PROP_SQL_ESCAPE_ALL);
        $this->readProperty($communicator, $buffer, self::PROP_SQL_INDEX_INLINE_MAX_SIZE);
        $this->readProperty($communicator, $buffer, self::PROP_SQL_SCHEMA);
        $this->readProperty($communicator, $buffer, self::PROP_WRITE_SYNCHRONIZATION_MODE);
        $this->readProperty($communicator, $buffer, self::PROP_CACHE_KEY_CONFIGURATION);
        $this->readProperty($communicator, $buffer, self::PROP_QUERY_ENTITY);
    }

    private function readProperty(BinaryCommunicator $communicator, MessageBuffer $buffer, int $propertyCode): void
    {
        $propertyType = self::$propInfo[$propertyCode];
        switch (BinaryUtils::getTypeCode($propertyType)) {
            case ObjectType::INTEGER:
            case ObjectType::LONG:
            case ObjectType::BOOLEAN:
                $this->properties[$propertyCode] = $communicator->readTypedObject($buffer, $propertyType);
                return;
            case ObjectType::STRING:
                $this->properties[$propertyCode] = $communicator->readObject($buffer, $propertyType);
                return;
            case ObjectType::OBJECT_ARRAY:
                $length = $buffer->readInteger();
                $properties = [];
                for ($i = 0; $i < $length; $i++) {
                    $propClassName = $propertyType->getElementType()->getPhpClassName();
                    $property = new $propClassName();
                    $property->read($communicator, $buffer);
                    array_push($properties, $property);
                }
                $this->properties[$propertyCode] = $properties;
                return;
            default:
                BinaryUtils::internalError();
        }
    }
}

CacheConfiguration::init();
