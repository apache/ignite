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

package org.apache.ignite.table;

/**
 * Invocation context provides access to invoke operation call parameters, a method to set a new value for the key.
 *
 * <p>InvokeProcessor executes atomically under lock which makes impossible to trigger 'live-schema' upgrade within the invoke operation.
 * Any try to update the row leading to schema change will end up with {@link InvokeProcessorException}.
 *
 * <p>New value MUST BE compliant with the current schema version.
 *
 * @param <K> Target object type.
 * @param <V> Value object type.
 * @see InvokeProcessor
 */
public interface InvocationContext<K, V> {
    /**
     * Returns arguments provided by user for invoke operation.
     *
     * @return Agruments for invocation processor.
     */
    Object[] args();

    /**
     * Returns an object the user provide to invoke call for running invoke processor against the associated row.
     *
     * <p>Depending on Table view the invoke operation is called on, the returning value is either value object or record object or tuple
     * with value fields set.
     *
     * @return Object which target row is associated with.
     */
    K key();

    /**
     * Returns current value object for the target row.
     *
     * <p>Depending on Table view the invoke operation is called on, the returning value is either value object or record object or tuple
     * with value fields set or {@code null} for non-existed row.
     *
     * @return Current value of target row or {@code null} if value associated with the key is not exists.
     */
    V value();

    /**
     * Sets a new value object for the target row.
     *
     * <p>Depending on Table view the invoke operation is called on, a new value can be either value object or record object or tuple with
     * value fields set or {@code null} for removal.
     *
     * @param val Value object to set.
     * @throws InvokeProcessorException if new value is not compliant with the current schema.
     */
    void value(V val);
}
