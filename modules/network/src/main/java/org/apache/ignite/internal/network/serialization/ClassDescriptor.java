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

import java.lang.reflect.Modifier;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Class descriptor for the user object serialization.
 */
public class ClassDescriptor {
    /**
     * Name of the class.
     */
    @NotNull
    private final String className;

    /**
     * Class.
     */
    @NotNull
    private final Class<?> clazz;

    /**
     * Descriptor id.
     */
    private final int descriptorId;

    /**
     * List of the class fields' descriptors.
     */
    @NotNull
    private final List<FieldDescriptor> fields;

    /**
     * The type of the serialization mechanism for the class.
     */
    private final int serializationType;

    /**
     * Whether the class is final.
     */
    private final boolean isFinal;

    /**
     * Constructor.
     */
    public ClassDescriptor(@NotNull Class<?> clazz, int descriptorId, @NotNull List<FieldDescriptor> fields, int serializationType) {
        this.className = clazz.getName();
        this.clazz = clazz;
        this.descriptorId = descriptorId;
        this.fields = List.copyOf(fields);
        this.serializationType = serializationType;
        this.isFinal = Modifier.isFinal(clazz.getModifiers());
    }

    /**
     * Returns descriptor id.
     *
     * @return Descriptor id.
     */
    public int descriptorId() {
        return descriptorId;
    }

    /**
     * Returns fields' descriptors.
     *
     * @return Fields' descriptors.
     */
    @NotNull
    public List<FieldDescriptor> fields() {
        return fields;
    }

    /**
     * Returns class' name.
     *
     * @return Class' name.
     */
    @NotNull
    public String className() {
        return className;
    }

    /**
     * Returns descriptor's class.
     *
     * @return Class.
     */
    @NotNull
    public Class<?> clazz() {
        return clazz;
    }

    /**
     * Returns serialization type.
     *
     * @return Serialization type.
     */
    public int serializationType() {
        return serializationType;
    }

    /**
     * Returns {@code true} if class is final, {@code false} otherwise.
     *
     * @return {@code true} if class is final, {@code false} otherwise.
     */
    public boolean isFinal() {
        return isFinal;
    }
}
