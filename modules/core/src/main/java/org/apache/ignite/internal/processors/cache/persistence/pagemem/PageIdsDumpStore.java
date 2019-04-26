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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public interface PageIdsDumpStore extends FullPageIdSource {
    /**
     * @return ID of newly created dump.
     */
    public String createDump();

    /**
     * Finish active dump.
     */
    public void finishDump();

    /**
     * @param partitions Partitions.
     */
    public void save(Iterable<Partition> partitions);

    /**
     * @param dumpId Dump ID.
     * @param consumer Consumer.
     * @param breakCond Break condition.
     */
    public void forEach(String dumpId, FullPageIdConsumer consumer, BooleanSupplier breakCond);

    /**
     *
     */
    public void clear();

    /**
     *
     */
    public static class Partition {
        /** */
        private final int id;

        /** */
        private final int cacheId;

        /** */
        private final int[] pageIndexes;

        /**
         * @param id Id.
         * @param cacheId Cache id.
         * @param pageIndexes Page indexes.
         */
        public Partition(int id, int cacheId, int[] pageIndexes) {
            this.id = id;
            this.cacheId = cacheId;
            this.pageIndexes = pageIndexes;
        }

        /**
         *
         */
        public int id() {
            return id;
        }

        /**
         *
         */
        public int cacheId() {
            return cacheId;
        }

        /**
         *
         */
        public int[] pageIndexes() {
            return pageIndexes;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(Partition.class, this, "pageIndexes", Arrays.toString(pageIndexes()));
        }
    }
}
