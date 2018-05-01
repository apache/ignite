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
 * ???
 */
class Timestamp {

    /**
     * Public constructor.
     *
     * @param {Date} date - ???.
     * @param {number} nanos - ???.
     *
     * @return {Timestamp} - new Timestamp instance
     */
    constructor(date, nanos) {
        this.setDate(date);
        this.setNanos(nanos);
    }

    /**
     * ???
     *
     * @return {Date} - ???.
     */
    getDate() {
        return this._date;
    }

    /**
     * ???
     *
     * @param {Date} date - ???.
     */
    setDate(date) {
        ArgumentChecker.notNull(date, 'date');
        ArgumentChecker.hasType(date, 'date', false, Date);
        this._date = date;
    }

    /**
     * ???
     *
     * @return {number} - ???.
     */
    getNanos() {
        return this._nanos;
    }

    /**
     * ???
     *
     * @param {number} nanos - ???.
     */
    setNanos(nanos) {
        ArgumentChecker.isInteger(nanos, 'nanos');
        this._nanos = nanos;
    }
}

module.exports = Timestamp;
