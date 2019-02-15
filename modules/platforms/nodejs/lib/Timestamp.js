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
