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

/**
 *
 */
public class ReflectionSample implements Sample {
    /** */
    private final Object sample;
    /** */
    private final List<Field> fields;

    /**
     * @param sample
     */
    public ReflectionSample(Object sample) throws SamplingException {
        assert sample != null;

        this.sample = sample;

        final List<Field> fields = new ArrayList<>();

        for (Class cls = sample.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
            for (java.lang.reflect.Field field : cls.getDeclaredFields()) {
                fields.add(new ReflectionField(field));
            }
        }

        this.fields = Collections.unmodifiableList(fields);
    }

    /** {@inheritDoc} */
    @Override public Object sample() {
        return sample;
    }

    /** {@inheritDoc} */
    @Override public String className() {
        return sample.getClass().getName();
    }

    /** {@inheritDoc} */
    @Override public Iterable<Field> fields() {
        return fields;
    }

    /**
     *
     */
    public class ReflectionField implements Field {
        /** */
        private final java.lang.reflect.Field field;

        /**
         * @param field
         */
        public ReflectionField(java.lang.reflect.Field field) {
            assert field != null;

            this.field = field;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return field.getName();
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return field.getType();
        }

        /** {@inheritDoc} */
        @Override public Object value() throws SamplingException {
            boolean accessible = field.isAccessible();

            field.setAccessible(true);

            try {
                return field.get(sample);
            }
            catch (IllegalAccessException e) {
                throw new SamplingException(e);
            }
            finally {
                if (!accessible)
                    field.setAccessible(false);
            }
        }

        /** {@inheritDoc} */
        @Override public void value(Object value) throws SamplingException {
            boolean accessible = field.isAccessible();

            field.setAccessible(true);

            try {
                field.set(sample, value);
            }
            catch (IllegalAccessException e) {
                throw new SamplingException(e);
            }
            finally {
                if (!accessible)
                    field.setAccessible(false);
            }
        }
    }
}
