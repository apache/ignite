<?php
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
