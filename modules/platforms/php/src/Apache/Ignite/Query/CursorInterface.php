<?php
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

namespace Apache\Ignite\Query;

/**
 * Interface representing a cursor to obtain results of SQL and Scan query operations.
 *
 * An instance of the class with this interface should be obtained via query() method
 * of an object with CacheInterface.
 * One instance of the class with CursorInterface returns results of one SQL or Scan query operation.
 *
 * CursorInterface extends the PHP Iterator interface.
 * The PHP Iterator's methods may be used to obtain the results of the query (cache entries, i.e. key-value pairs)
 * one by one.
 * Also, the cursor can be placed into the "foreach" PHP loop to easily iterate over all the results.
 *
 * Additionally, CursorInterface includes getAll() method to get all the results at once
 * and close() method to prematurely close the cursor.
 *
 */
interface CursorInterface extends \Iterator
{
    /**
     * Returns all elements (cache entries, i.e. key-value pairs) from the query results at once.
     *
     * May be used instead of the PHP Iterator's methods if the number of returned entries
     * is relatively small and will not cause memory utilization issues.
     *
     * @return array all cache entries (key-value pairs) returned by SQL or Scan query.
     */
    public function getAll(): array;

    /**
     * Closes the cursor. Obtaining elements from the results is not possible after this.
     *
     * This method should be called if no more elements are needed.
     * It is not necessary to call it if all elements have been already obtained.
     */
    public function close(): void;
}
