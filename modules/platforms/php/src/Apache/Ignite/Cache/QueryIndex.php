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

namespace Apache\Ignite\Cache;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Utils\ArgumentChecker;

/**
 * Class representing one Query Index element of QueryEntity of Ignite CacheConfiguration.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class QueryIndex
{
    /** @name QueryIndexType
     *  @anchor QueryIndexType
     *  @{
     */
    const TYPE_SORTED = 0;
    const TYPE_FULLTEXT = 1;
    const TYPE_GEOSPATIAL = 2;
    /** @} */ // end of QueryIndexType

    private $name;
    private $type;
    private $inlineSize;
    private $fields;

    /**
     * QueryIndex constructor.
     *
     * @param string|null $name
     * @param int $type one of @ref QueryIndexType constants.
     *
     * @throws ClientException if error.
     */
    public function __construct(string $name = null, int $type = self::TYPE_SORTED)
    {
        $this->name = $name;
        $this->setType($type);
        $this->inlineSize = -1;
        $this->fields = null;
    }

    /**
     *
     *
     * @param string $name
     *
     * @return QueryIndex the same instance of the QueryIndex.
     */
    public function setName(string $name): QueryIndex
    {
        $this->name = $name;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getName(): ?string
    {
        return $this->name;
    }

    /**
     *
     *
     * @param int $type one of @ref QueryIndexType constants.
     *
     * @return QueryIndex the same instance of the QueryIndex.
     *
     * @throws ClientException if error.
     */
    public function setType(int $type): QueryIndex
    {
        ArgumentChecker::hasValueFrom(
            $type, 'type', false, [self::TYPE_SORTED, self::TYPE_FULLTEXT, self::TYPE_GEOSPATIAL]);
        $this->type = $type;
        return $this;
    }

    /**
     *
     *
     * @return int index type, one of @ref QueryIndexType constants.
     */
    public function getType(): int
    {
        return $this->type;
    }

    /**
     *
     *
     * @param int $inlineSize
     *
     * @return QueryIndex the same instance of the QueryIndex.
     */
    public function setInlineSize(int $inlineSize): QueryIndex
    {
        $this->inlineSize = $inlineSize;
        return $this;
    }

    /**
     *
     *
     * @return int
     */
    public function getInlineSize(): int
    {
        return $this->inlineSize;
    }

    /**
     *
     *
     * @param array[string => bool] $fields
     *
     * @return QueryIndex the same instance of the QueryIndex.
     */
    public function setFields(array $fields): QueryIndex
    {
        $this->fields = $fields;
        return $this;
    }

    /**
     *
     *
     * @return array[string => bool]|null
     */
    public function getFields(): ?array
    {
        return $this->fields;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        BinaryCommunicator::writeString($buffer, $this->name);
        $buffer->writeByte($this->type);
        $buffer->writeInteger($this->inlineSize);
        // write fields
        $length = $this->fields ? count($this->fields) : 0;
        $buffer->writeInteger($length);
        if ($length > 0) {
            foreach ($this->fields as $key => $value) {
                BinaryCommunicator::writeString($buffer, $key);
                $buffer->writeBoolean($value);
            }
        }
    }

    // This is not the public API method, is not intended for usage by an application.
    public function read(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        $this->name = BinaryCommunicator::readString($buffer);
        $this->type = $buffer->readByte();
        $this->inlineSize = $buffer->readInteger();
        // read fields
        $length = $buffer->readInteger();
        $this->fields = [];
        if ($length > 0) {
            $this->fields[BinaryCommunicator::readString($buffer)] = $buffer->readBoolean();
        }
    }
}
