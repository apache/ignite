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

package org.apache.ignite.internal.marshaller;

import java.util.function.Supplier;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * Marshaller column.
 */
public class MarshallerColumn {
    /** Default "default value supplier". */
    private static final Supplier<Object> NULL_SUPPLIER = () -> null;

    /**
     * Column name.
     */
    private final String name;

    /**
     * Column type.
     */
    private final BinaryMode type;

    /**
     * Default value supplier.
     */
    @IgniteToStringExclude
    private final Supplier<Object> defValSup;

    /**
     * Constructor.
     *
     * @param name      Column name.
     * @param type      An instance of column data type.
     */
    public MarshallerColumn(String name, BinaryMode type) {
        this(name, type, null);
    }

    /**
     * Constructor.
     *
     * @param name      Column name.
     * @param type      An instance of column data type.
     * @param defValSup Default value supplier.
     */
    public MarshallerColumn(String name, BinaryMode type, @Nullable Supplier<Object> defValSup) {
        this.name = name;
        this.type = type;
        this.defValSup = defValSup == null ? NULL_SUPPLIER : defValSup;
    }

    public String name() {
        return name;
    }

    public BinaryMode type() {
        return type;
    }

    public Object defaultValue() {
        return defValSup.get();
    }
}
