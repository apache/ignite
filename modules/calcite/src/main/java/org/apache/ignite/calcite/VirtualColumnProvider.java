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

package org.apache.ignite.calcite;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;

/**
 * Virtual table column provider from {@link PluginProvider plugin} created via
 * {@link PluginProvider#createComponent(PluginContext, Class)} for Calcite-based query engine.
 */
@FunctionalInterface
@IgniteExperimental
public interface VirtualColumnProvider {
    /** */
    VirtualColumnProvider EMPTY = List::of;

    /**
     * Returns a list of virtual column descriptions to add to tables.
     *
     * <p>NOTES:</p>
     * <ul>
     *     <li>{@link VirtualColumnDescriptor#name()} - it is recommended to return in uppercase.</li>
     *     <li>{@link VirtualColumnDescriptor#name()} - it is forbidden to use system names {@code "_KEY"} and
     *     {@code "_VAL"}.</li>
     *     <li>User will get an error when trying to create a column with the name of one of the virtual ones.</li>
     * </ul>
     */
    List<VirtualColumnDescriptor> provideVirtualColumnDescriptors();

    /** Virtual column descriptor. */
    interface VirtualColumnDescriptor {
        /** */
        int NOT_SPECIFIED = -1;

        /** Returns the name of the virtual column. */
        String name();

        /** Virtual column type. */
        Class<?> type();

        /** Returns the scale of the virtual column type, {@value #NOT_SPECIFIED} if not specified. */
        int scale();

        /** Returns the precision of the virtual column type, {@value #NOT_SPECIFIED} if not specified. */
        int precision();

        /**
         * Returns the value of a virtual column.
         *
         * @param ctx Context to extract the value.
         * @throws IgniteCheckedException If there are errors when getting the value.
         */
        Object value(ValueExtractorContext ctx) throws IgniteCheckedException;
    }

    /** Context for extracting the value of a virtual column. */
    interface ValueExtractorContext {
        /** Returns the cache ID. */
        int cacheId();

        /** Returns cache name. */
        String cacheName();

        /** Returns the partition ID. */
        int partition();

        /**
         * Returns the source for getting the value of the virtual column.
         *
         * @param keyOrValue {@code true} if a cache key (primary key) is needed, {@code false} if a cache value is
         *      needed.
         * @param keepBinary {@code true} if returned as {@link BinaryObject}. If the key or value is not composite, it
         *      may return not as {@link BinaryObject}, but as a simple type.
         */
        Object source(boolean keyOrValue, boolean keepBinary);
    }
}
