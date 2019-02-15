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

namespace Apache\Ignite\Internal\Query;

use Apache\Ignite\Query\SqlFieldsCursorInterface;
use Apache\Ignite\Internal\Binary\ClientOperation;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\BinaryUtils;

class SqlFieldsCursor extends Cursor implements SqlFieldsCursorInterface
{
    private $fieldCount;
    private $fieldNames;
    private $fieldTypes;
    
    public function __construct(BinaryCommunicator $communicator, MessageBuffer $buffer)
    {
        parent::__construct($communicator, ClientOperation::QUERY_SQL_FIELDS_CURSOR_GET_PAGE, $buffer);
        $this->fieldCount = 0;
        $this->fieldNames = [];
        $this->fieldTypes = null;
    }

    public function getFieldNames(): array
    {
        return $this->fieldNames;
    }

    public function setFieldTypes(...$fieldTypes): SqlFieldsCursorInterface
    {
        foreach ($fieldTypes as $fieldType) {
            BinaryUtils::checkObjectType($fieldType, 'fieldTypes');
        }
        $this->fieldTypes = $fieldTypes;
        return $this;
    }

    public function readFieldNames(MessageBuffer $buffer, bool $includeFieldNames): void
    {
        $this->id = $buffer->readLong();
        $this->fieldCount = $buffer->readInteger();
        if ($includeFieldNames) {
            for ($i = 0; $i < $this->fieldCount; $i++) {
                array_push($this->fieldNames, $this->communicator->readObject($buffer));
            }
        }
    }

    protected function readRow(MessageBuffer $buffer)
    {
        $values = [];
        for ($i = 0; $i < $this->fieldCount; $i++) {
            $fieldType = $this->fieldTypes && $i < count($this->fieldTypes) ? $this->fieldTypes[$i] : null;
            array_push($values, $this->communicator->readObject($buffer, $fieldType));
        }
        return $values;
    }
}
