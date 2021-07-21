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

package org.apache.ignite.table.mapper;

/**
 * Mappers shortcut methods.
 */
public final class Mappers {
    /**
     * Factory method for key mapper.
     *
     * @param cls Key class.
     * @param <K> Key type.
     * @return Mapper for key.
     */
    public static <K> KeyMapper<K> ofKeyClass(Class<K> cls) {
        return null;
    }

    /**
     * Factory method for value mapper.
     *
     * @param cls Value class.
     * @param <V> Value type.
     * @return Mapper for value.
     */
    public static <V> ValueMapper<V> ofValueClass(Class<V> cls) {
        return null;
    }

    /**
     * Factory method for value mapper builder.
     *
     * @param cls Value class.
     * @param <V> Value type.
     * @return Mapper builder for value.
     */
    public static <V> ValueMapper.Builder<V> ofValueClassBuilder(Class<V> cls) {
        return null;
    }

    /**
     * Identity key mapper.
     *
     * @param <R> Record type.
     * @return Identity key mapper.
     */
    public static <R> KeyMapper<R> identity() {
        return null;
    }

    /**
     * Factory method for record mapper builder.
     *
     * @param cls Record class.
     * @param <R> Record type.
     * @return Mapper builder for record.
     */
    public static <R> RecordMapper<R> ofRecordClass(Class<R> cls) {
        return null;
    }

    /**
     * Factory method for value mapper builder.
     *
     * @param cls Record class.
     * @param <R> Record type.
     * @return Mapper builder for record.
     */
    public static <R> RecordMapper.Builder<R> ofRecordClassBuilder(Class<R> cls) {
        return null;
    }

    /**
     * Stub.
     */
    private Mappers() {
    }
}
