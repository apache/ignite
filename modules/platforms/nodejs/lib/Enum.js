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
class Enum {

    /**
     * Public constructor.
     *
     * @param {number} typeId - ???.
     * @param {number} ordinal - ???.
     *
     * @return {Enum} - new Enum instance
     */
    constructor(typeId, ordinal) {
        this.setTypeId(typeId);
        this.setOrdinal(ordinal);
    }

    /**
     * ???
     *
     * @return {number} - ???.
     */
    getTypeId() {
        return this._typeId;
    }

    /**
     * ???
     *
     * @param {number} typeId - ???.
     */
    setTypeId(typeId) {
        ArgumentChecker.isInteger(typeId, 'typeId');
        this._typeId = typeId;
    }

    /**
     * ???
     *
     * @return {number} - ???.
     */
    getOrdinal() {
        return this._ordinal;
    }

    /**
     * ???
     *
     * @param {number} ordinal - ???.
     */
    setOrdinal(ordinal) {
        ArgumentChecker.isInteger(ordinal, 'ordinal');
        this._ordinal = ordinal;
    }
}

module.exports = Enum;
