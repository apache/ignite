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

'use strict';

const ArgumentChecker = require('./internal/ArgumentChecker');

/**
 * Class representing an Ignite timestamp type.
 *
 * The timestamp extends the standard JavaScript {@link Date} Object and consists of:
 *   - time  - the number of milliseconds since January 1, 1970, 00:00:00 UTC,
 *     methods of the JavaScript {@link Date} Object can be used to operate with the time.
 *   - nanoseconds - fraction of the last millisecond in the range from 0 to 999999 nanoseconds,
 *     this class specifies additional methods to operate with the nanoseconds.
 * @extends Date
 */
class Timestamp extends Date {

    /**
     * Public constructor.
     *
     * @param {number} time - integer value representing the number of milliseconds since January 1, 1970, 00:00:00 UTC.
     * @param {number} nanos - integer value representing the nanoseconds of the last millisecond,
     *                         should be in the range from 0 to 999999.
     *
     * @return {Timestamp} - new Timestamp instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(time, nanos) {
        super(time);
        this.setNanos(nanos);
    }

    /**
     * Returns the nanoseconds of the last millisecond from the timestamp.
     *
     * @return {number} - nanoseconds of the last millisecond.
     */
    getNanos() {
        return this._nanos;
    }

    /**
     * Updates the nanoseconds of the last millisecond in the timestamp.
     *
     * @param {number} nanos - new value for the nanoseconds of the last millisecond,
     *                         should be in the range from 0 to 999999.
     *
     * @return {Timestamp} - the same instance of Timestamp
     *
     * @throws {IgniteClientError} if error.
     */
    setNanos(nanos) {
        ArgumentChecker.isInteger(nanos, 'nanos');
        this._nanos = nanos;
        return this;
    }
}

module.exports = Timestamp;
