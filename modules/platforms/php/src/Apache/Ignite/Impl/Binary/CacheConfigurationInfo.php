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

namespace Apache\Ignite\Impl\Binary;

use Apache\Ignite\Type\ObjectType;

class CacheConfigurationInfo
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
    
    private static $types;
    
    public static function getType(int $propertyCode)
    {
        return CacheConfigurationInfo::$types[$propertyCode];
    }
    
    static function init(): void
    {
        CacheConfigurationInfo::$types = array(
            CacheConfigurationInfo::PROP_NAME => ObjectType::STRING,
            CacheConfigurationInfo::PROP_SQL_SCHEMA => ObjectType::STRING,
        );
    }
}

CacheConfigurationInfo::init();

//const PROP_TYPES = Object.freeze({
//    [PROP_NAME] : BinaryUtils.TYPE_CODE.STRING,
//    [PROP_CACHE_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_ATOMICITY_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_BACKUPS] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_WRITE_SYNCHRONIZATION_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_COPY_ON_READ] : BinaryUtils.TYPE_CODE.BOOLEAN,
//    [PROP_READ_FROM_BACKUP] : BinaryUtils.TYPE_CODE.BOOLEAN,
//    [PROP_DATA_REGION_NAME] : BinaryUtils.TYPE_CODE.STRING,
//    [PROP_IS_ONHEAP_CACHE_ENABLED] : BinaryUtils.TYPE_CODE.BOOLEAN,
//    [PROP_QUERY_ENTITY] : new ObjectArrayType(new ComplexObjectType(new QueryEntity())),
//    [PROP_QUERY_PARALLELISM] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_QUERY_DETAIL_METRICS_SIZE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_SQL_SCHEMA] : BinaryUtils.TYPE_CODE.STRING,
//    [PROP_SQL_INDEX_INLINE_MAX_SIZE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_SQL_ESCAPE_ALL] : BinaryUtils.TYPE_CODE.BOOLEAN,
//    [PROP_MAX_QUERY_ITERATORS] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_REBALANCE_MODE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_REBALANCE_DELAY] : BinaryUtils.TYPE_CODE.LONG,
//    [PROP_REBALANCE_TIMEOUT] : BinaryUtils.TYPE_CODE.LONG,
//    [PROP_REBALANCE_BATCH_SIZE] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_REBALANCE_BATCHES_PREFETCH_COUNT] : BinaryUtils.TYPE_CODE.LONG,
//    [PROP_REBALANCE_ORDER] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_REBALANCE_THROTTLE] : BinaryUtils.TYPE_CODE.LONG,
//    [PROP_GROUP_NAME] : BinaryUtils.TYPE_CODE.STRING,
//    [PROP_CACHE_KEY_CONFIGURATION] : new ObjectArrayType(new ComplexObjectType(new CacheKeyConfiguration())),
//    [PROP_DEFAULT_LOCK_TIMEOUT] : BinaryUtils.TYPE_CODE.LONG,
//    [PROP_MAX_CONCURRENT_ASYNC_OPS] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_PARTITION_LOSS_POLICY] : BinaryUtils.TYPE_CODE.INTEGER,
//    [PROP_EAGER_TTL] : BinaryUtils.TYPE_CODE.BOOLEAN,
//    [PROP_STATISTICS_ENABLED] : BinaryUtils.TYPE_CODE.BOOLEAN
//});
