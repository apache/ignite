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
 * Class representing a timestamp.
 *
 * The timestamp consists of:
 *   - date - a standard JavaScript {@link Date}.
 *   - nanoseconds - fraction of the last second, which value could be in the range from 0 to 999999999.
 */
class Timestamp extends Date {

    /**
     * Public constructor.
     *
     * @param {number} time - integer value representing the number of milliseconds since January 1, 1970, 00:00:00 UTC.
     * @param {number} nanos - nanoseconds of the last second.
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
     * Returns the nanoseconds of the last second from the timestamp.
     *
     * @return {number} - nanoseconds of the last second.
     */
    getNanos() {
        return this._nanos;
    }

    /**
     * Updates the nanoseconds of the last second in the timestamp without changing the date.
     *
     * @param {number} nanos - new value for the nanoseconds of the last second.
     *
     * @throws {IgniteClientError} if error.
     */
    setNanos(nanos) {
        ArgumentChecker.isInteger(nanos, 'nanos');
        this._nanos = nanos;
    }
}

module.exports = Timestamp;
