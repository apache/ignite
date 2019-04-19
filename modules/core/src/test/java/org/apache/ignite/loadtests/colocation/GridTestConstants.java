/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.loadtests.colocation;

/**
 * Set of constants for this project.
 */
public class GridTestConstants {
    /** Number of modulo regions to partition keys into. */
    public static final int MOD_COUNT = 1024;

    /** Number of entries to put in cache. */
    public static final int ENTRY_COUNT = 2000000;

    /** Cache init size - add some padding to avoid resizing. */
    public static final int CACHE_INIT_SIZE = (int)(1.5 * ENTRY_COUNT);

    /** Number of threads to load cache. */
    public static final int LOAD_THREADS = Runtime.getRuntime().availableProcessors() * 2;
}