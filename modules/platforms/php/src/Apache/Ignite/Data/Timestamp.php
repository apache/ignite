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

namespace Apache\Ignite\Data;

use \DateTime;

/** 
 * Class representing Ignite Timestamp type
 * (Ignite Date with additional nanoseconds fraction of the last millisecond).
 */
class Timestamp extends Date
{
    private $nanos;

    /**
     * Public constructor.
     * 
     * @param float $millis integer number of milliseconds elapsed since January 1, 1970, 00:00:00 UTC.
     * @param int $nanos nanoseconds of the last millisecond, should be in the range from 0 to 999999.
     */
    public function __construct(float $millis, int $nanos)
    {
        parent::__construct($millis);
        $this->nanos = $nanos;
    }
    
    /**
     * Creates Timestamp instance from DateTime instance.
     * 
     * @param DateTime $dateTime DateTime instance.
     * 
     * @return Timestamp new Timestamp instance.
     */
    public static function fromDateTime(DateTime $dateTime)
    {
        $micros = $dateTime->format('u');
        $millis = intval($micros / 1000);
        return new Timestamp($dateTime->getTimestamp() * 1000 + $millis, ($micros % 1000) * 1000);
    }
    
    /**
     * Returns the nanoseconds of the last millisecond from the timestamp.
     * 
     * @return int nanoseconds of the last millisecond.
     */
    public function getNanos(): int
    {
        return $this->nanos;
    }
}
