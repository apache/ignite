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

import _ from 'lodash';

// List of H2 reserved SQL keywords.
import H2_SQL_KEYWORDS from 'app/data/sql-keywords.json';

// List of JDBC type descriptors.
import JDBC_TYPES from 'app/data/jdbc-types.json';

// Regular expression to check H2 SQL identifier.
const VALID_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_$]*$/im;

// Descriptor for unknown JDBC type.
const UNKNOWN_JDBC_TYPE = {
    dbName: 'Unknown',
    signed: {javaType: 'Unknown', primitiveType: 'Unknown'},
    unsigned: {javaType: 'Unknown', primitiveType: 'Unknown'}
};

/**
 * Utility service for various check on SQL types.
 */
export default class SqlTypes {
    /**
     * @param {String} value Value to check.
     * @returns {boolean} 'true' if given text is valid Java class name.
     */
    validIdentifier(value) {
        return !!(value && VALID_IDENTIFIER.test(value));
    }

    /**
     * @param value {String} Value to check.
     * @returns {boolean} 'true' if given text is one of H2 reserved keywords.
     */
    isKeyword(value) {
        return !!(value && _.includes(H2_SQL_KEYWORDS, value.toUpperCase()));
    }

    /**
     * Find JDBC type descriptor for specified JDBC type and options.
     *
     * @param {Number} dbType  Column db type.
     * @return {String} Java type.
     */
    findJdbcType(dbType) {
        const jdbcType = _.find(JDBC_TYPES, (item) => item.dbType === dbType);

        return jdbcType ? jdbcType : UNKNOWN_JDBC_TYPE;
    }
}
