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

use Apache\Ignite\Cache\CacheEntry;
use Apache\Ignite\Query\CursorInterface;
use Apache\Ignite\Internal\Binary\ClientOperation;
use Apache\Ignite\Internal\Binary\MessageBuffer;
use Apache\Ignite\Internal\Binary\BinaryCommunicator;

class Cursor implements CursorInterface
{
    protected $communicator;
    private $operation;
    private $buffer;
    private $keyType;
    private $valueType;
    protected $id;
    private $hasNext;
    private $values;
    private $valueIndex;
    private $rewinds;
    private $index;
    
    public function __construct(BinaryCommunicator $communicator, int $operation, MessageBuffer $buffer, $keyType = null, $valueType = null)
    {
        $this->communicator = $communicator;
        $this->operation = $operation;
        $this->buffer = $buffer;
        $this->keyType = $keyType;
        $this->valueType = $valueType;
        $this->id = null;
        $this->hasNext = false;
        $this->values = null;
        $this->valueIndex = 0;
        $this->rewinds = 0;
        $this->index = 0;
    }

    public function current()
    {
        return $this->values[$this->valueIndex];
    }
    
    public function key()
    {
        return $this->index;
    }
    
    public function next() 
    {
        $this->valueIndex++;
        $this->index++;
    }
    
    public function rewind(): void
    {
        $this->rewinds++;
    }

    public function valid(): bool
    {
        if ($this->rewinds > 1) {
            return false;
        }
        if (!$this->values || $this->valueIndex >= count($this->values)) {
            $this->obtainValues();
            $this->valueIndex = 0;
        }
        return $this->values && $this->valueIndex < count($this->values);
    }

    public function getAll(): array
    {
        $result = [];
        foreach ($this as $value) {
            array_push($result, $value);
        }
        return $result;
    }

    public function close(): void
    {
        // Close cursor only if the server has more pages: the server closes cursor automatically on last page
        if ($this->id && $this->hasNext) {
            $this->communicator->send(
                ClientOperation::RESOURCE_CLOSE,
                function (MessageBuffer $payload)
                {
                    $this->write($payload);
                });
        }
    }

    private function getNext(): void
    {
        $this->hasNext = false;
        $this->values = null;
        $this->buffer = null;
        $this->communicator->send(
            $this->operation,
            function (MessageBuffer $payload)
            {
                $this->write($payload);
            },
            function (MessageBuffer $payload)
            {
                $this->buffer = $payload;
            });
    }

    private function obtainValues(): void
    {
        if (!$this->buffer && $this->hasNext) {
            $this->getNext();
        }
        $this->values = null;
        if ($this->buffer) {
            $this->read($this->buffer);
            $this->buffer = null;
        }
    }

    private function write(MessageBuffer $buffer): void
    {
        $buffer->writeLong($this->id);
    }

    public function readId(MessageBuffer $buffer): void
    {
        $this->id = $buffer->readLong();
    }

    protected function readRow(MessageBuffer $buffer)
    {
        return new CacheEntry(
            $this->communicator->readObject($buffer, $this->keyType),
            $this->communicator->readObject($buffer, $this->valueType));
    }

    private function read(MessageBuffer $buffer): void
    {
        $rowCount = $buffer->readInteger();
        $this->values = [];
        for ($i = 0; $i < $rowCount; $i++) {
            array_push($this->values, $this->readRow($buffer));
        }
        $this->hasNext = $buffer->readBoolean();
    }
}
