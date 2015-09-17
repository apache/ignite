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

package org.apache.ignite.internal.processors.query;

import java.util.Collection;

/**
 * Describes an index to be created for a certain type. It contains all necessary
 * information about fields, order, uniqueness, and specified
 * whether this is SQL or Text index.
 * See also {@link GridQueryTypeDescriptor#indexes()}.
 */
public interface GridQueryIndexDescriptor {
    /**
     * Gets all fields to be indexed.
     *
     * @return Fields to be indexed.
     */
    public Collection<String> fields();

    /**
     * Specifies order of the index for each indexed field.
     *
     * @param field Field name.
     * @return {@code True} if given field should be indexed in descending order.
     */
    public boolean descending(String field);

    /**
     * Gets index type.
     *
     * @return Type.
     */
    public GridQueryIndexType type();
}