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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.asm.AsmSerializerGenerator;
import org.apache.ignite.internal.schema.marshaller.reflection.JavaSerializerFactory;

/**
 * (De)Serializer factory interface.
 */
@FunctionalInterface
public interface SerializerFactory {
    /**
     * @return Serializer factory back by code generator.
     */
    public static SerializerFactory createGeneratedSerializerFactory() {
        return new AsmSerializerGenerator();
    }

    /**
     * @return Reflection-based serializer factory.
     */
    public static SerializerFactory createJavaSerializerFactory() {
        return new JavaSerializerFactory();
    }

    /**
     * Creates serializer.
     *
     * @param schema Schema descriptor.
     * @param keyClass Key class.
     * @param valClass Value class.
     * @return Serializer.
     */
    public Serializer create(SchemaDescriptor schema, Class<?> keyClass, Class<?> valClass);
}
