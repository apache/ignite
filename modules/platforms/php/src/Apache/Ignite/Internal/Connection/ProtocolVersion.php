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

