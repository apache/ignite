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

package org.apache.ignite.ml.math;

/**
 * Support for different modes of accessing data storage.
 */
public interface StorageConstants {
    /** Storage mode optimized for sequential access. */
    public static final int SEQUENTIAL_ACCESS_MODE = 1001;

    /** Storage mode optimized for random access. */
    public static final int RANDOM_ACCESS_MODE = 1002;

    /** Storage mode optimized for row access. */
    public static final int ROW_STORAGE_MODE = 2001;

    /** Storage mode optimized for column access. */
    public static final int COLUMN_STORAGE_MODE = 2002;

    /** Storage mode is unknown. */
    public static final int UNKNOWN_STORAGE_MODE = 3001;

    /**
     * @param mode Access mode to verify.
     */
    public default void assertAccessMode(int mode) {
        assert mode == SEQUENTIAL_ACCESS_MODE || mode == RANDOM_ACCESS_MODE;
    }

    /**
     * @param mode Storage mode to verify.
     */
    public default void assertStorageMode(int mode) {
        assert mode == ROW_STORAGE_MODE || mode == COLUMN_STORAGE_MODE;
    }
}
