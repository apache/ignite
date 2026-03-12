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

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshallers;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Query field metadata.
 */
public class QueryField implements Serializable, Message {
    /** */
    private static final long serialVersionUID = 0L;

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

    /** Default value. */
    private Object dfltVal;

    /** Serialized form of 'default value'. */
    @Order(value = 4, method = "defaultValueBytes")
    transient byte[] dfltValBytes;

    /** Precision. */
    @Order(5)
    int precision;

    /** Scale. */
    @Order(6)
    int scale;

    /** */
    public QueryField() { }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     */
    public QueryField(String name, String typeName, boolean nullable) {
        this(name, typeName, nullable, null, -1, -1);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     * @param dfltVal Default value.
     */
    public QueryField(String name, String typeName, boolean nullable, Object dfltVal) {
        this(name, typeName, nullable, dfltVal, -1, -1);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     * @param dfltVal Default value.
     * @param precision Precision.
     * @param scale Scale.
     */
    public QueryField(String name, String typeName, boolean nullable, Object dfltVal, int precision, int scale) {
        this(name, typeName, null, nullable, dfltVal, precision, scale);
    }

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param alias Alias.
     * @param nullable Nullable flag.
     * @param dfltVal Default value.
     * @param precision Precision.
     * @param scale Scale.
     */
    public QueryField(String name, String typeName, String alias, boolean nullable, Object dfltVal, int precision, int scale) {
        this.name = name;
        this.typeName = typeName;
        this.alias = alias;
        this.nullable = nullable;
        this.dfltVal = dfltVal;
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
     * @return Default value.
     */
    public Object defaultValue() {
        return dfltVal;
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

    /** */
    public byte[] defaultValueBytes() {
        try {
            return U.marshal(Marshallers.jdk(), dfltVal);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to marshal default value", e);
        }
    }

    /** */
    public void defaultValueBytes(byte[] dfltValBytes) {
        if (dfltValBytes != null) {
            try {
                dfltVal = U.unmarshal(Marshallers.jdk(), dfltValBytes, U.gridClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal default value", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryField.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -110;
    }
}
