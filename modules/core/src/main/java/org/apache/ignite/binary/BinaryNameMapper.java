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

package org.apache.ignite.binary;

import org.apache.ignite.configuration.BinaryConfiguration;

/**
 * Maps type and field names to different names. Prepares class/type names
 * and field names before pass them to {@link BinaryIdMapper}.
 * <p>
 * Binary name mapper can be configured for all binary objects via
 * {@link BinaryConfiguration#getNameMapper()} method,
 * or for a specific binary type via {@link BinaryTypeConfiguration#getNameMapper()} method.
 * @see BinaryIdMapper
 */
public interface BinaryNameMapper {
    /**
     * Gets type clsName.
     *
     * @param clsName Class came
     * @return Type name.
     */
    String typeName(String clsName);

    /**
     * Gets field name.
     *
     * @param fieldName Field name.
     * @return Field name.
     */
    String fieldName(String fieldName);
}
