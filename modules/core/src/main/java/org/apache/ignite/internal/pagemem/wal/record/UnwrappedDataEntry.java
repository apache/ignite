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

package org.apache.ignite.internal.pagemem.wal.record;

/**
 * Interface for Data Entry for automatic unwrapping key and value from Data Entry
 */
public interface UnwrappedDataEntry {
    /**
     * Unwraps key value from cache key object into primitive boxed type or source class. If client classes were used in
     * key, call of this method requires classes to be available in classpath.
     *
     * @return Key which was placed into cache. Or null if failed to convert.
     */
    Object unwrappedKey();

    /**
     * Unwraps value value from cache value object into primitive boxed type or source class. If client classes were
     * used in key, call of this method requires classes to be available in classpath.
     *
     * @return Value which was placed into cache. Or null for delete operation or for failure.
     */
    Object unwrappedValue();
}
