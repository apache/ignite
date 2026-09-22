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

package org.apache.ignite.internal.ducktest.tests.mdc;

/** Operation modes shared by the MDC load applications. */
public enum LoadMode {
    /** Cache API reads with value verification. */
    GET,

    /** Cache API writes. */
    PUT,

    /** Transactional cache API writes (requires a TRANSACTIONAL cache). */
    TX_PUT,

    /** SQL reads with value verification (requires an SQL-enabled cache). */
    SQL_SELECT,

    /** SQL DML writes (requires an SQL-enabled cache). */
    SQL_PUT;

    /** @return Whether the mode writes to the cache. */
    public boolean isWrite() {
        return this == PUT || this == TX_PUT || this == SQL_PUT;
    }

    /** @return Whether the mode operates through SQL. */
    public boolean isSql() {
        return this == SQL_SELECT || this == SQL_PUT;
    }
}
