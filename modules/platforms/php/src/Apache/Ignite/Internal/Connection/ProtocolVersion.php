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

namespace Apache\Ignite\Internal\Connection;

use Apache\Ignite\Internal\Binary\MessageBuffer;

class ProtocolVersion
{
    public static $V_1_2_0;

    private $major;
    private $minor;
    private $patch;

    public static function init(): void
    {
        ProtocolVersion::$V_1_2_0 = new ProtocolVersion(1, 2, 0);
    }

    public function __construct(int $major = 0, int $minor = 0, int $patch = 0)
    {
        $this->major = $major;
        $this->minor = $minor;
        $this->patch = $patch;
    }

    public function compareTo(ProtocolVersion $other): int
    {
        $diff = $this->major - $other->major;
        if ($diff !== 0) {
            return $diff;
        }
        $diff = $this->minor - $other->minor;
        if ($diff !== 0) {
            return $diff;
        }
        return $this->patch - $other->patch;
    }

    public function equals(ProtocolVersion $other): bool
    {
        return $this->compareTo($other) === 0;
    }

    public function toString(): string
    {
        return sprintf('%d.%d.%d', $this->major, $this->minor, $this->patch);
    }

    public function read(MessageBuffer $buffer): void
    {
        $this->major = $buffer->readShort();
        $this->minor = $buffer->readShort();
        $this->patch = $buffer->readShort();
    }

    public function write(MessageBuffer $buffer): void
    {
        $buffer->writeShort($this->major);
        $buffer->writeShort($this->minor);
        $buffer->writeShort($this->patch);
    }
}

ProtocolVersion::init();

