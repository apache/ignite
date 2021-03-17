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

package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.ColumnType;

/**
 * Alter column builder.
 *
 * NOTE: Only safe actions that can be applied automatically on-fly are allowed.
 */
public interface AlterColumnBuilder {
    /**
     * Renames a column.
     *
     * @param newName New column name.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder withNewName(String newName);

    /**
     * Convert column to a new type.
     * <p>
     * Note: New type must be compatible with old.
     *
     * @param newType New column type.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder convertTo(ColumnType newType);

    /**
     * Sets new column default value.
     *
     * @param defaultValue Default value.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder withNewDefault(Object defaultValue);

    /**
     * Mark column as nullable.
     *
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder asNullable();

    /**
     * Mark column as non-nullable.
     *
     * Note: Replacement param is mandatory, all previously stored 'nulls'
     * will be treated as replacement value on read.
     *
     * @param replacement Non-null value, that 'null' will be converted to.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder asNonNullable(Object replacement);

    /**
     * @return Parent builder.
     */
    TableModificationBuilder done();
}
