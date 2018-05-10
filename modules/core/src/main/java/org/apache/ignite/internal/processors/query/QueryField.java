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

import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;

/**
 * Query field metadata.
 */
public class QueryField implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field name. */
    private final String name;

    /** Class name for this field's values. */
    private final String typeName;

    /** Nullable flag. */
    private final boolean nullable;

    /** Default value. */
    private final Object dfltValue;

    /** Precision. */
    private final int precision;

    /** Scale. */
    private final int scale;

    /** Max length. */
    private final int maxLength;

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     */
    public QueryField(String name, String typeName, boolean nullable) {
        this(name, typeName, nullable, null, -1, -1, Integer.MAX_VALUE);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     * @param dfltValue Default value.
     */
    public QueryField(String name, String typeName, boolean nullable, Object dfltValue) {
        this(name, typeName, nullable, dfltValue, -1, -1, Integer.MAX_VALUE);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     * @param dfltValue Default value.
     */
    public QueryField(String name, String typeName, boolean nullable, Object dfltValue, int precision, int scale, 
        int maxLength) {
        this.name = name;
        this.typeName = typeName;
        this.nullable = nullable;
        this.dfltValue = dfltValue;
        this.precision = precision;
        this.scale = scale;
        this.maxLength = maxLength;
    }

    /**
     * @return Field name.
     */
    public String name() {
        return name;
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
     * @return Default value.
     */
    public Object defaultValue() {
        return dfltValue;
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

    /**
     * @return Maximum length.
     */
    public int maxLength() {
        return maxLength;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryField.class, this);
    }
}
