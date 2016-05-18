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

package org.apache.ignite.cache.store.cassandra.persistence;

/**
 * Describes persistence strategy to be used to persist object data into Cassandra.
 */
public enum PersistenceStrategy {
    /**
     * Stores object value as is, by mapping its value to Cassandra table column with corresponding type.
     * <p>
     * Could be used for primitive java type (like Integer, String, Long and etc) which could be directly mapped
     * to appropriate Cassandra types.
     */
    PRIMITIVE,

    /**
     * Stores object value as BLOB, by mapping its value to Cassandra table column with blob type.
     * Could be used for any java type. Conversion of java object to BLOB is handled by specified serializer.
     * <p>
     * Available serializer implementations:
     * <ul>
     *     <li>
     *         org.apache.ignite.cache.store.cassandra.serializer.JavaSerializer - uses standard Java
     *         serialization framework.
     *     </li>
     *     <li>
     *        org.apache.ignite.cache.store.cassandra.serializer.KryoSerializer - uses Kryo serialization
     *        framework.
     *     </li>
     * </ul>
     */
    BLOB,

    /**
     * Stores each field of an object as a column having corresponding type in Cassandra table.
     * Provides ability to utilize Cassandra secondary indexes for object fields.
     * <p>
     * Could be used for objects which follow JavaBeans convention and having empty public constructor.
     * Object fields should be:
     * <ul>
     *     <li>Primitive java types like int, long, String and etc.</li>
     *     <li>Collections of primitive java types like List<Integer>, Map<Integer, String>, Set<Long></li>
     * </ul>
     */
    POJO
}
