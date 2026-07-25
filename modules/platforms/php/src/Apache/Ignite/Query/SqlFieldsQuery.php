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
use Apache\Ignite\Internal\Query\SqlFieldsCursor;

/**
 * Class representing an SQL Fields query.
 */
class SqlFieldsQuery extends SqlQuery
{
    /** @name SqlFieldsQueryStatementType
     *  @anchor SqlFieldsQueryStatementType
     *  @{
     */
    const STATEMENT_TYPE_ANY = 0;
    const STATEMENT_TYPE_SELECT = 1;
    const STATEMENT_TYPE_UPDATE = 2;
    /** @} */ // end of SqlFieldsQueryStatementType
    
    private $schema;
    private $maxRows;
    private $statementType;
    private $enforceJoinOrder;
    private $collocated;
    private $lazy;
    private $includeFieldNames;
    
    /**
     * Public constructor.
     *
     * Requires SQL query string to be specified.
     * Other SQL Fields query settings have the following defaults:
     * <pre>
     *     SQL Fields Query setting  :    Default value
     *     Local query flag          :    false
     *     Cursor page size          :    1024
     *     Query arguments           :    not specified
     *     Distributed joins flag    :    false
     *     Replicated only flag      :    false
     *     Timeout                   :    0 (disabled)
     *     Schema for the query      :    not specified
     *     Max rows                  :    -1
     *     Statement type            :    STATEMENT_TYPE_ANY
     *     Enforce join order flag   :    false
     *     Collocated flag           :    false
     *     Lazy query execution flag :    false
     *     Include field names flag  :    false
     * </pre>
     * Every setting may be changed using set methods.
     * 
     * @param string $sql SQL query string.
     *
     * @throws ClientException if error.
     */
    public function __construct(string $sql)
    {
        parent::__construct(null, $sql);
        $this->operation = ClientOperation::QUERY_SQL_FIELDS;
        $this->schema = null;
        $this->maxRows = -1;
        $this->statementType = SqlFieldsQuery::STATEMENT_TYPE_ANY;
        $this->enforceJoinOrder = false;
        $this->collocated = false;
        $this->lazy = false;
        $this->includeFieldNames = false;
    }

    /**
     * Set schema for the query.
     * 
     * @param string $schema schema for the query.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setSchema(string $schema): SqlFieldsQuery
    {
        $this->schema = $schema;
        return $this;
    }

    /**
     * Set max rows.
     * 
     * @param int $maxRows max rows.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setMaxRows(int $maxRows): SqlFieldsQuery
    {
        $this->maxRows = $maxRows;
        return $this;
    }

    /**
     * Set statement type.
     * 
     * @param int $type statement type, one of @ref SqlFieldsQueryStatementType constants.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setStatementType(int $type): SqlFieldsQuery
    {
        $this->statementType = $type;
        return $this;
    }

    /**
     * Set enforce join order flag.
     * 
     * @param bool $enforceJoinOrder enforce join order flag: true or false.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setEnforceJoinOrder(bool $enforceJoinOrder): SqlFieldsQuery
    {
        $this->enforceJoinOrder = $enforceJoinOrder;
        return $this;
    }

    /**
     * Set collocated flag.
     * 
     * @param bool $collocated collocated flag: true or false.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setCollocated(bool $collocated): SqlFieldsQuery
    {
        $this->collocated = $collocated;
        return $this;
    }

    /**
     * Set lazy query execution flag.
     * 
     * @param bool $lazy lazy query execution flag: true or false.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setLazy(bool $lazy): SqlFieldsQuery
    {
        $this->lazy = $lazy;
        return $this;
    }

    /**
     * Set include field names flag.
     * 
     * @param bool $includeFieldNames include field names flag: true or false.
     * 
     * @return SqlFieldsQuery the same instance of the SqlFieldsQuery.
     */
    public function setIncludeFieldNames(bool $includeFieldNames): SqlFieldsQuery
    {
        $this->includeFieldNames = $includeFieldNames;
        return $this;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        BinaryCommunicator::writeString($buffer, $this->schema);
        $buffer->writeInteger($this->pageSize);
        $buffer->writeInteger($this->maxRows);
        BinaryCommunicator::writeString($buffer, $this->sql);
        $this->writeArgs($communicator, $buffer);
        $buffer->writeByte($this->statementType);
        $buffer->writeBoolean($this->distributedJoins);
        $buffer->writeBoolean($this->local);
        $buffer->writeBoolean($this->replicatedOnly);
        $buffer->writeBoolean($this->enforceJoinOrder);
        $buffer->writeBoolean($this->collocated);
        $buffer->writeBoolean($this->lazy);
        $buffer->writeLong($this->timeout);
        $buffer->writeBoolean($this->includeFieldNames);
    }

    // This is not the public API method, is not intended for usage by an application.
    public function getCursor(BinaryCommunicator $communicator, MessageBuffer $payload, $keyType = null, $valueType = null): CursorInterface
    {
        $cursor = new SqlFieldsCursor($communicator, $payload);
        $cursor->readFieldNames($payload, $this->includeFieldNames);
        return $cursor;
    }
}
