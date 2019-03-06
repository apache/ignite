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

package org.apache.ignite.internal.client.thin;

/** Operation codes. */
enum ClientOperation {
    /** Resource close. */RESOURCE_CLOSE(0),
    /** Cache get or create with name. */CACHE_GET_OR_CREATE_WITH_NAME(1052),
    /** Cache put. */CACHE_PUT(1001),
    /** Cache get. */CACHE_GET(1000),
    /** Cache create with configuration. */CACHE_CREATE_WITH_CONFIGURATION(1053),
    /** Cache get names. */CACHE_GET_NAMES(1050),
    /** Cache destroy. */CACHE_DESTROY(1056),
    /** Cache get or create with configuration. */CACHE_GET_OR_CREATE_WITH_CONFIGURATION(1054),
    /** Cache create with name. */CACHE_CREATE_WITH_NAME(1051),
    /** Cache contains key. */CACHE_CONTAINS_KEY(1011),
    /** Cache get configuration. */CACHE_GET_CONFIGURATION(1055),
    /** Get size. */CACHE_GET_SIZE(1020),
    /** Put all. */CACHE_PUT_ALL(1004),
    /** Get all. */CACHE_GET_ALL(1003),
    /** Cache replace if equals. */CACHE_REPLACE_IF_EQUALS(1010),
    /** Cache replace. */CACHE_REPLACE(1009),
    /** Cache remove key. */CACHE_REMOVE_KEY(1016),
    /** Cache remove if equals. */CACHE_REMOVE_IF_EQUALS(1017),
    /** Cache remove keys. */CACHE_REMOVE_KEYS(1018),
    /** Cache remove all. */CACHE_REMOVE_ALL(1019),
    /** Cache get and put. */CACHE_GET_AND_PUT(1005),
    /** Cache get and remove. */CACHE_GET_AND_REMOVE(1007),
    /** Cache get and replace. */CACHE_GET_AND_REPLACE(1006),
    /** Cache put if absent. */CACHE_PUT_IF_ABSENT(1002),
    /** Cache clear. */CACHE_CLEAR(1013),
    /** Query scan. */QUERY_SCAN(2000),
    /** Query scan cursor get page. */QUERY_SCAN_CURSOR_GET_PAGE(2001),
    /** Query sql. */QUERY_SQL(2002),
    /** Query sql cursor get page. */QUERY_SQL_CURSOR_GET_PAGE(2003),
    /** Query sql fields. */QUERY_SQL_FIELDS(2004),
    /** Query sql fields cursor get page. */QUERY_SQL_FIELDS_CURSOR_GET_PAGE(2005),
    /** Get binary type. */GET_BINARY_TYPE(3002),
    /** Register binary type name. */REGISTER_BINARY_TYPE_NAME(3001),
    /** Put binary type. */PUT_BINARY_TYPE(3003),
    /** Get binary type name. */GET_BINARY_TYPE_NAME(3000);

    /** Code. */
    private final int code;

    /** Constructor. */
    ClientOperation(int code) {
        this.code = code;
    }

    /**
     * @return Code.
     */
    public short code() {
        return (short)code;
    }
}
