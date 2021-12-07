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

package org.apache.ignite.internal.network.serialization;

import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;

/**
 * Field descriptor for the user object serialization.
 */
public class FieldDescriptor {
    /**
     * Name of the field.
     */
    @NotNull
    private final String name;

    /**
     * Type of the field.
     */
    @NotNull
    private final Class<?> clazz;

    /**
     * Field type's descriptor id.
     */
    private final int typeDescriptorId;

    /**
     * Constructor.
     */
    public FieldDescriptor(@NotNull Field field, int typeDescriptorId) {
        this.name = field.getName();
        this.clazz = field.getType();
        this.typeDescriptorId = typeDescriptorId;
    }

    /**
     * Returns field's name.
     *
     * @return Field's name.
     */
    @NotNull
    public String name() {
        return name;
    }

    /**
     * Returns field's type.
     *
     * @return Field's type.
     */
    @NotNull
    public Class<?> clazz() {
        return clazz;
    }

    /**
     * Returns field's type descriptor id.
     *
     * @return Field's type descriptor id.
     */
    public int typeDescriptorId() {
        return typeDescriptorId;
    }
}
