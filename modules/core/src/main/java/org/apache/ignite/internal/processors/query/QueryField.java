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

    /**
     * @param name Field name.
     * @param typeName Class name for this field's values.
     * @param nullable Nullable flag.
     */
    public QueryField(String name, String typeName, boolean nullable) {
        this.name = name;
        this.typeName = typeName;
        this.nullable = nullable;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryField.class, this);
    }
}
