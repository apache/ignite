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

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Query field metadata.
 */
public class QueryField implements Message {
    /** Field name. */
    @Order(0)
    String name;

    /** Alias. */
    @Order(1)
    String alias;

    /** Class name for this field's values. */
    @Order(2)
    String typeName;

    /** Nullable flag. */
    @Order(3)
    boolean nullable;

    /** Precision. */
    @Order(4)
    int precision;

    /** Scale. */
    @Order(5)
    int scale;

    /** */
    public QueryField() { }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     */
    public QueryField(String name, String typeName, boolean nullable) {
        this(name, typeName, nullable, -1, -1);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     * @param precision Precision.
     * @param scale Scale.
     */
    public QueryField(String name, String typeName, boolean nullable, int precision, int scale) {
        this(name, typeName, null, nullable, precision, scale);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param alias Alias.
     * @param nullable Nullable flag.
     * @param precision Precision.
     * @param scale Scale.
     */
    public QueryField(String name, String typeName, String alias, boolean nullable, int precision, int scale) {
        this.name = name;
        this.typeName = typeName;
        this.alias = alias;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * @return Field name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Field alias.
     */
    public String alias() {
        return alias != null ? alias : name;
    }

    /**
     * @return Class name for this field's values.
     */
    public String typeName() {
        return typeName;
    }

    /**
     * @return {@code true}, if field is nullable.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return Precision.
     */
    public int precision() {
        return precision;
    }

    /**
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryField.class, this);
    }
}
