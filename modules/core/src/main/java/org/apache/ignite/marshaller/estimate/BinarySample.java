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

package org.apache.ignite.marshaller.estimate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;

/**
 *
 */
public class BinarySample implements Sample {
    /** */
    private final BinaryObjectBuilder builder;
    /** */
    private final String className;
    /** */
    private final List<Field> fields;

    /**
     * @param binary
     * @param className
     * @param fieldTypes
     * @throws SamplingException
     */
    public BinarySample(
        IgniteBinary binary,
        String className,
        Map<String, String> fieldTypes) throws SamplingException {

        try {
            this.builder = binary.builder(className);
        }
        catch (IgniteException e) {
            throw new SamplingException(e);
        }

        this.className = className;

        final List<Field> fields = new ArrayList<>();

        for (Map.Entry<String, String> fieldType : fieldTypes.entrySet()) {
            fields.add(new BinaryField<>(
                fieldType.getKey(),
                typeNameToClass(fieldType.getValue()))
            );
        }

        this.fields = Collections.unmodifiableList(fields);
    }

    /**
     * @param typeName Type name.
     * @return Class for name {@code typeName}.
     * @throws SamplingException On failure.
     */
    private static Class<?> typeNameToClass(String typeName) throws SamplingException {
        switch (typeName) {
            case "boolean":
                return Boolean.TYPE;
            case "byte":
                return Byte.TYPE;
            case "char":
                return Character.TYPE;
            case "short":
                return Short.TYPE;
            case "int":
                return Integer.TYPE;
            case "long":
                return Long.TYPE;
            case "float":
                return Float.TYPE;
            case "double":
                return Double.TYPE;
        }

        try {
            return Class.forName(typeName);
        }
        catch (ClassNotFoundException e) {
            throw new SamplingException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Object sample() {
        return builder.build();
    }

    /** {@inheritDoc} */
    @Override public String className() {
        return className;
    }

    /** {@inheritDoc} */
    @Override public Iterable<Field> fields() {
        return fields;
    }

    /**
     * @param <T>
     */
    public class BinaryField<T> implements Field {
        /** */
        private final String name;
        /** */
        private final Class<T> type;
        /** */
        private T val;

        /**
         * @param name
         * @param type
         */
        public BinaryField(String name, Class<T> type) {
            this.name = name;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public Class<T> type() {
            return type;
        }

        /** {@inheritDoc} */
        @Override public Object value() throws SamplingException {
            return val;
        }

        /** {@inheritDoc} */
        @Override public void value(Object val) throws SamplingException {
            T typedVal = type.cast(val);
            builder.setField(name, typedVal, type);
            this.val = typedVal;
        }
    }
}
