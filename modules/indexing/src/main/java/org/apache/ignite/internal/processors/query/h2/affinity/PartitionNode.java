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

package org.apache.ignite.internal.processors.query.h2.affinity;

import java.util.Arrays;
import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Common node of partition tree.
 */
public interface PartitionNode {
    /**
     * Get partitions.
     *
     * @param args Query arguments.
     * @return Partitions.
     * @throws IgniteCheckedException If failed.
     */
    Collection<Integer> apply(Object... args) throws IgniteCheckedException;

    /**
     * Try optimizing partition nodes into a simpler form.
     *
     * @return Optimized node or {@code this} if optimization failed.
     */
    default PartitionNode optimize() {
        return this;
    }

    /**
     * Get partitions.
     *
     * @param args Query arguments.
     * @return Partitions.
     */
    default int[] calculateDerivedPartitions(String sql, Object... args) {
        try {
            int[] parts;
            Collection<Integer> partitions0 = apply(args);

            if (F.isEmpty(partitions0))
                parts = new int[0];
            else {
                parts = new int[partitions0.size()];

                int i = 0;

                for (Integer part : partitions0)
                    parts[i++] = part;
            }

            return parts;
        }
        catch (IgniteCheckedException e) {
            throw new CacheException("Failed to calculate derived partitions: [qry=" + sql +
                ", params=" + Arrays.deepToString(args) + "]", e);
        }
    }
}
