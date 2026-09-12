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

use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\ClientOperation;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\BinaryUtils;
use Apache\Ignite\Internal\Query\Cursor;
use Apache\Ignite\Internal\Utils\ArgumentChecker;

/**
 * Class representing an SQL query which returns the whole cache entries (key-value pairs).
 */
class SqlQuery extends Query
{
    private $args;
    private $argTypes;
    protected $sql;
    protected $type;
    protected $distributedJoins;
    protected $replicatedOnly;
    protected $timeout;

    /**
     * Public constructor.
     *
     * Requires name of a type (or SQL table) and SQL query string to be specified.
     * Other SQL query settings have the following defaults:
     * <pre>
     *     SQL Query setting         :    Default value
     *     Local query flag          :    false
     *     Cursor page size          :    1024
     *     Query arguments           :    not specified
     *     Distributed joins flag    :    false
     *     Replicated only flag      :    false
     *     Timeout                   :    0 (disabled)
     * </pre>
     * Every setting may be changed using set methods.
     *
     * @param string $type name of a type or SQL table.
     * @param string $sql SQL query string.
     * 
     * @throws ClientException if error.
     */
    public function __construct(?string $type, string $sql)
    {
        parent::__construct(ClientOperation::QUERY_SQL);
        $this->setType($type);
        $this->setSql($sql);
        $this->args = null;
        $this->argTypes = null;
        $this->distributedJoins = false;
        $this->replicatedOnly = false;
        $this->timeout = 0;
    }

    /**
     * Set name of a type or SQL table.
     *
     * @param string $type name of a type or SQL table.
     *
     * @return SqlQuery the same instance of the SqlQuery.
     *
     * @throws ClientException if error.
     */
    public function setType(?string $type): SqlQuery
    {
        if ($this instanceof SqlFieldsQuery) {
            ArgumentChecker::invalidArgument($type, 'type', SqlFieldsQuery::class);
        }
        $this->type = $type;
        return $this;
    }

    /**
     * Set SQL query string.
     * 
     * @param string $sql SQL query string.
     * 
     * @return SqlQuery the same instance of the SqlQuery.
     */
    public function setSql(string $sql): SqlQuery
    {
        $this->sql = $sql;
        return $this;
    }

    /**
     * Set query arguments.
     *
     * Type of any argument may be specified using setArgTypes() method.
     * If type of an argument is not specified then during operations the Ignite client
     * will try to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param mixed ...$args Query arguments.
     * 
     * @return SqlQuery the same instance of the SqlQuery.
     */
    public function setArgs(...$args): SqlQuery
    {
        $this->args = $args;
        return $this;
    }

    /**
     * Specifies types of query arguments.
     *
     * Query arguments itself are set using setArgs() method.
     * By default, a type of every argument is not specified that means during operations the Ignite client
     * will try to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param int|ObjectType|null ...$argTypes types of Query arguments.
     *   The order of types must follow the order of arguments in the setArgs() method.
     *   A type of every argument can be:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * 
     * @return SqlQuery the same instance of the SqlQuery.
     *
     * @throws ClientException if error.
     */
    public function setArgTypes(...$argTypes): SqlQuery
    {
        foreach ($argTypes as $argType) {
            BinaryUtils::checkObjectType($argType, 'argTypes');
        }
        $this->argTypes = $argTypes;
        return $this;
    }

    /**
     * Set distributed joins flag.
     * 
     * @param bool $distributedJoins distributed joins flag: true or false.
     * 
     * @return SqlQuery the same instance of the SqlQuery.
     */
    public function setDistributedJoins(bool $distributedJoins): SqlQuery
    {
        $this->distributedJoins = $distributedJoins;
        return $this;
    }

    /**
     * Set replicated only flag.
     * 
     * @param bool $replicatedOnly replicated only flag: true or false.
     * 
     * @return SqlQuery the same instance of the SqlQuery.
     */
    public function setReplicatedOnly(bool $replicatedOnly): SqlQuery
    {
        $this->replicatedOnly = $replicatedOnly;
        return $this;
    }

    /**
     * Set timeout.
     * 
     * @param float $timeout timeout value in milliseconds.
     *   Must be non-negative. Zero value disables timeout.
     * 
     * @return SqlQuery the same instance of the SqlQuery.
     */
    public function setTimeout(float $timeout): SqlQuery
    {
        $this->timeout = $timeout;
        return $this;
    }

    protected function writeArgs(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        $argsLength = $this->args ? count($this->args) : 0;
        $buffer->writeInteger($argsLength);
        if ($argsLength > 0) {
            for ($i = 0; $i < $argsLength; $i++) {
                $argType = $this->argTypes && $i < count($this->argTypes) ? $this->argTypes[$i] : null;
                $communicator->writeObject($buffer, $this->args[$i], $argType);
            }
        }
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        BinaryCommunicator::writeString($buffer, $this->type);
        BinaryCommunicator::writeString($buffer, $this->sql);
        $this->writeArgs($communicator, $buffer);
        $buffer->writeBoolean($this->distributedJoins);
        $buffer->writeBoolean($this->local);
        $buffer->writeBoolean($this->replicatedOnly);
        $buffer->writeInteger($this->pageSize);
        $buffer->writeLong($this->timeout);
    }

    // This is not the public API method, is not intended for usage by an application.
    public function getCursor(BinaryCommunicator $communicator, MessageBuffer $payload, $keyType = null, $valueType = null): CursorInterface
    {
        $cursor = new Cursor($communicator, ClientOperation::QUERY_SQL_CURSOR_GET_PAGE, $payload, $keyType, $valueType);
        $cursor->readId($payload);
        return $cursor;
    }
}
