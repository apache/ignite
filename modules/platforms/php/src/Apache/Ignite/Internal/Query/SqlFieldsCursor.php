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
