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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;

import org.apache.ignite.internal.schema.NativeType;

/**
 * Metadata of the field of a query result set.
 */
public interface ResultFieldMetadata {
    /**
     * @return name of the result's field.
     */
    String name();

    /**
     * @return index (order) of the result's field (starts from 0).
     */
    int order();

    /**
     * @return type of the result's field.
     */
    NativeType type();

    /**
     * @return nullable flag of the result's field.
     */
    boolean isNullable();

    /**
     * Example:
     * SELECT SUM(price), category, subcategory FROM Goods WHERE [condition] GROUP_BY category, subcategory;
     *
     * Field - Origin
     * SUM(price) - null;
     * category - {"PUBLIC", "Goods", "category"};
     * subcategory - {"PUBLIC", "Goods", "subcategory"};
     *
     * @return field's origin (or where a field value comes from).
     */
    List<String> origin();
}
