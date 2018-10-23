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

namespace Apache\Ignite\Data;

/** 
 * Class representing Ignite Time type
 * (number of milliseconds elapsed since midnight, i.e. 00:00:00 UTC).
 */
class Time
{
    private $millis;

    /**
     * Public constructor.
     * 
     * @param int $millis number of milliseconds elapsed since midnight, i.e. 00:00:00 UTC.
     */
    public function __construct(int $millis)
    {
        $this->millis = $millis;
    }
    
    /**
     * Returns the time value as number of milliseconds elapsed since midnight, i.e. 00:00:00 UTC.
     * 
     * @return int number of milliseconds elapsed since midnight, i.e. 00:00:00 UTC.
     */
    public function getMillis(): int
    {
        return $this->millis;
    }

    /**
     * Returns the time value as number of seconds elapsed since midnight, i.e. 00:00:00 UTC.
     * 
     * @return int number of seconds elapsed since midnight, i.e. 00:00:00 UTC.
     */
    public function getSeconds(): int
    {
        return $this->millis / 1000;
    }
}
