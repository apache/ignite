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

package org.apache.ignite.internal.configuration.tree;

import org.jetbrains.annotations.Nullable;

/**
 * Configuration source.
 */
public interface ConfigurationSource {
    /**
     * Treat current configuration source as a leaf value and try to convert it to the specific class.
     * Failing behaviour is not specified and depends on the implementation.
     *
     * @param <T> Type of the object for type safety during compilation.
     * @param clazz Class instance of type to convert to.
     * @return Converted leaf object.
     */
    default <T> T unwrap(Class<T> clazz) {
        throw new UnsupportedOperationException("unwrap");
    }

    /**
     * Treats current configuration source as an inner node. Tries to construct the content of {@code node} using
     * available data from the source.
     *
     * @param node Constructable node which content will be modified by the configuration source.
     */
    default void descend(ConstructableTreeNode node) {
    }

    /**
     * Reset internal state, preparing this configuration source for reuse.
     */
    default void reset() {
    }

    /**
     * Returns the identifier of the polymorphic configuration instance.
     *
     * @param fieldName Name of the field in the configuration schema that should store the identifier of the polymorphic configuration instance.
     * @return Identifier of the polymorphic configuration instance.
     */
    @Nullable default String polymorphicTypeId(String fieldName) {
        return null;
    }
}
