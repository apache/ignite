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
 * 
 */
interface SqlFieldsCursorInterface extends CursorInterface
{
    /**
     * Returns all elements (arrays with values of the fields) from the query results.
     *
     * May be used instead of getValue() method if the number of returned elements
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
     * Specifies types of the fields returned by the SQL Fields query.
     *
     * By default, a type of every field is not specified that means during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param int|ObjectType|null ...$fieldTypes types of the returned fields.
     *   The order of types must correspond the order of field values returned in the results of the query.
     *   A type of every field can be:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return SqlFieldsCursorInterface - the same instance of the SqlFieldsCursorInterface.
     */
    public function setFieldTypes(...$fieldTypes): SqlFieldsCursorInterface;
}
