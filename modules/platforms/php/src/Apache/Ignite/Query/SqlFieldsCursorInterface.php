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

use Apache\Ignite\Type\ObjectType;

/**
 * Interface representing a cursor to obtain results of SQL Fields query operation.
 *
 * An instance of the class with this interface should be obtained via query() method
 * of an object with CacheInterface.
 * One instance of the class with SqlFieldsCursorInterface returns results of one SQL Fields query operation.
 *
 * SqlFieldsCursorInterface extends CursorInterface which extends the PHP Iterator interface.
 * The PHP Iterator's methods may be used to obtain the results of the query (arrays with values of the fields)
 * one by one.
 * Also, the cursor can be placed into the "foreach" PHP loop to easily iterate over all the results.
 *
 * Additionally, SqlFieldsCursorInterface includes
 * getAll() method to get all the results at once,
 * getFieldNames() method to return names of the fields,
 * setFieldTypes() method to specify Ignite types of the fields,
 * and close() method (defined in CursorInterface) to prematurely close the cursor.
 *
 */
interface SqlFieldsCursorInterface extends CursorInterface
{
    /**
     * Returns all elements (arrays with values of the fields) from the query results at once.
     *
     * May be used instead of the PHP Iterator's methods if the number of returned elements
     * is relatively small and will not cause memory utilization issues.
     *
     * @return array all results returned by SQL Fields query.
     *   Every element of the array is an array with values of the fields requested by the query.
     */
    public function getAll(): array;
    
    /**
     * Returns names of the fields which were requested in the SQL Fields query.
     *
     * Empty array is returned if "include field names" flag was false in the query.
     *
     * @return array field names.
     *   The order of names corresponds to the order of field values returned in the results of the query.
     */
    public function getFieldNames(): array;

    /**
     * Specifies Ignite types of the fields returned by the SQL Fields query.
     *
     * By default, an Ignite type of every field is not specified that means during operations Ignite client
     * tries to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     *
     * @param int|ObjectType|null ...$fieldTypes Ignite types of the returned fields.
     *   The order of types must correspond the order of field values returned in the results of the query.
     *   A type of every field can be:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return SqlFieldsCursorInterface the same instance of the class with SqlFieldsCursorInterface.
     */
    public function setFieldTypes(...$fieldTypes): SqlFieldsCursorInterface;
}
