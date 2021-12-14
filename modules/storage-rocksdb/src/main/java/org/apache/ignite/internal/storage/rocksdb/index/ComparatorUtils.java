/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb.index;

import java.util.Comparator;
import java.util.function.Function;

class ComparatorUtils {
    /**
     * Creates a comparator similar to {@link Comparator#comparing(Function, Comparator)}, but allows the key extractor functions
     * to return {@code null}.
     *
     * <p>Null values are always treated as smaller than the non-null values.
     */
    static <T, U> Comparator<T> comparingNull(Function<? super T, ? extends U> keyExtractor, Comparator<? super U> keyComparator) {
        return (o1, o2) -> {
            U key1 = keyExtractor.apply(o1);
            U key2 = keyExtractor.apply(o2);

            if (key1 == key2) {
                return 0;
            } else if (key1 == null) {
                return -1;
            } else if (key2 == null) {
                return 1;
            } else {
                return keyComparator.compare(key1, key2);
            }
        };
    }
}
