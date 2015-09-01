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

package org.apache.ignite.internal.processors.hadoop;

import org.jetbrains.annotations.Nullable;

/**
 * Enumeration of optional properties supported by Ignite for Apache Hadoop.
 */
public enum HadoopJobProperty {
    /**
     * Initial size for hashmap which stores output of mapper and will be used as input of combiner.
     * <p>
     * Setting it right allows to avoid rehashing.
     */
    COMBINER_HASHMAP_SIZE,

    /**
     * Initial size for hashmap which stores output of mapper or combiner and will be used as input of reducer.
     * <p>
     * Setting it right allows to avoid rehashing.
     */
    PARTITION_HASHMAP_SIZE,

    /**
     * Specifies number of concurrently running mappers for external execution mode.
     * <p>
     * If not specified, defaults to {@code Runtime.getRuntime().availableProcessors()}.
     */
    EXTERNAL_CONCURRENT_MAPPERS,

    /**
     * Specifies number of concurrently running reducers for external execution mode.
     * <p>
     * If not specified, defaults to {@code Runtime.getRuntime().availableProcessors()}.
     */
    EXTERNAL_CONCURRENT_REDUCERS,

    /**
     * Delay in milliseconds after which Ignite server will reply job status.
     */
    JOB_STATUS_POLL_DELAY,

    /**
     * Size in bytes of single memory page which will be allocated for data structures in shuffle.
     * <p>
     * By default is {@code 32 * 1024}.
     */
    SHUFFLE_OFFHEAP_PAGE_SIZE,

    /**
     * If set to {@code true} then input for combiner will not be sorted by key.
     * Internally hash-map will be used instead of sorted one, so {@link Object#equals(Object)}
     * and {@link Object#hashCode()} methods of key must be implemented consistently with
     * comparator for that type. Grouping comparator is not supported if this setting is {@code true}.
     * <p>
     * By default is {@code false}.
     */
    SHUFFLE_COMBINER_NO_SORTING,

    /**
     * If set to {@code true} then input for reducer will not be sorted by key.
     * Internally hash-map will be used instead of sorted one, so {@link Object#equals(Object)}
     * and {@link Object#hashCode()} methods of key must be implemented consistently with
     * comparator for that type. Grouping comparator is not supported if this setting is {@code true}.
     * <p>
     * By default is {@code false}.
     */
    SHUFFLE_REDUCER_NO_SORTING;

    /** */
    private final String ptyName;

    /**
     *
     */
    HadoopJobProperty() {
        ptyName = "ignite." + name().toLowerCase().replace('_', '.');
    }

    /**
     * @return Property name.
     */
    public String propertyName() {
        return ptyName;
    }

    /**
     * @param jobInfo Job info.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static String get(HadoopJobInfo jobInfo, HadoopJobProperty pty, @Nullable String dflt) {
        String res = jobInfo.property(pty.propertyName());

        return res == null ? dflt : res;
    }

    /**
     * @param jobInfo Job info.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static int get(HadoopJobInfo jobInfo, HadoopJobProperty pty, int dflt) {
        String res = jobInfo.property(pty.propertyName());

        return res == null ? dflt : Integer.parseInt(res);
    }

    /**
     * @param jobInfo Job info.
     * @param pty Property.
     * @param dflt Default value.
     * @return Property value.
     */
    public static boolean get(HadoopJobInfo jobInfo, HadoopJobProperty pty, boolean dflt) {
        String res = jobInfo.property(pty.propertyName());

        return res == null ? dflt : Boolean.parseBoolean(res);
    }
}