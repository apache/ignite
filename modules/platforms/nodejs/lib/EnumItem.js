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
 * Class representing an item of enum.
 *
 * The item is defined by:
 *   - type Id (mandatory) - Id of the enum type.
 *   - ordinal (optional) - ordinal of the item in the enum type.
 *   - name (optional) - name of the item.
 *   - value (optional) - value of the item.
 * Usually, at least one from the optional ordinal, name or value must be specified
 * in order to use an instance of this class.
 *
 */
class EnumItem {

    /**
     * Public constructor.
     *
     * @param {number} typeId - Id of the enum type.
     *
     * @return {EnumItem} - new EnumItem instance
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(typeId) {
        this.setTypeId(typeId);
        this._ordinal = null;
        this._name = null;
        this._value = null;
    }

    /**
     * Returns Id of the enum type.
     *
     * @return {number} - Id of the enum type.
     */
    getTypeId() {
        return this._typeId;
    }

    /**
     * Updates Id of the enum type.
     *
     * @param {number} typeId - new Id of the enum type.
     *
     * @throws {IgniteClientError} if error.
     */
    setTypeId(typeId) {
        ArgumentChecker.isInteger(typeId, 'typeId');
        this._typeId = typeId;
    }

    /**
     * Returns ordinal of the item in the enum type
     * or null if ordinal is not set.
     *
     * @return {number} - ordinal of the item in the enum type.
     */
    getOrdinal() {
        return this._ordinal;
    }

    /**
     * Sets or updates ordinal of the item in the enum type.
     *
     * @param {number} ordinal - ordinal of the item in the enum type.
     *
     * @throws {IgniteClientError} if error.
     */
    setOrdinal(ordinal) {
        ArgumentChecker.isInteger(ordinal, 'ordinal');
        this._ordinal = ordinal;
    }

    /**
     * Returns name of the item
     * or null if name is not set.
     *
     * @return {string} - name of the item.
     */
    getName() {
        return this._name;
    }

    /**
     * Sets or updates name of the item.
     *
     * @param {string} name - name of the item.
     *
     * @throws {IgniteClientError} if error.
     */
    setName(name) {
        ArgumentChecker.notEmpty(name, 'name');
        this._name = name;
    }

    /**
     * Returns value of the item
     * or null if value is not set.
     *
     * @return {number} - value of the item.
     */
    getValue() {
        return this._value;
    }

    /**
     * Sets or updates value of the item.
     *
     * @param {number} value - value of the item.
     *
     * @throws {IgniteClientError} if error.
     */
    setValue(value) {
        ArgumentChecker.isInteger(value, 'value');
        this._value = value;
    }
}

module.exports = EnumItem;
