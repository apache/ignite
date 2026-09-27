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

namespace Apache\Ignite\Query;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\ClientOperation;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Query\Cursor;

/**
 * Class representing a Scan query which returns the whole cache entries (key-value pairs).
 *
 * This version of the class does not support a possibility to specify a Filter object for the query.
 * The query returns all entries from the entire cache or from the specified partition.
 */
class ScanQuery extends Query
{
    private $partitionNumber;
    
    /**
     * Public constructor.
     *
     * Scan query settings have the following defaults:
     * <pre>
     *     Scan Query setting        :    Default value
     *     Local query flag          :    false
     *     Cursor page size          :    1024
     *     Partition number          :    -1 (entire cache)
     *     Filter object             :    null (not supported)
     * </pre>
     * Every setting (except Filter object) may be changed using set methods.
     */
    public function __construct()
    {
        parent::__construct(ClientOperation::QUERY_SCAN);
        $this->partitionNumber = -1;
    }

    /**
     * Sets a partition number over which this query should iterate.
     *
     * If negative, the query will iterate over all partitions in the cache. 
     * 
     * @param int $partitionNumber partition number over which this query should iterate.
     * 
     * @return ScanQuery the same instance of the ScanQuery.
     */
    public function setPartitionNumber(int $partitionNumber): ScanQuery
    {
        $this->partitionNumber = $partitionNumber;
        return $this;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        // filter
        $communicator->writeObject($buffer, null);
        $buffer->writeInteger($this->pageSize);
        $buffer->writeInteger($this->partitionNumber);
        $buffer->writeBoolean($this->local);
    }

    // This is not the public API method, is not intended for usage by an application.
    public function getCursor(BinaryCommunicator $communicator, MessageBuffer $payload, $keyType = null, $valueType = null): CursorInterface
    {
        $cursor = new Cursor($communicator, ClientOperation::QUERY_SCAN_CURSOR_GET_PAGE, $payload, $keyType, $valueType);
        $cursor->readId($payload);
        return $cursor;
    }
}
