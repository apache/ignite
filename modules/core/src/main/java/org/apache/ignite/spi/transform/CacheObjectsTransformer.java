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

package org.apache.ignite.spi.transform;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Cache objects transformer.
 */
@IgniteExperimental
public interface CacheObjectsTransformer extends Serializable {
    /** Additional space required to store the transformed data. */
    public int OVERHEAD = 6;

    /**
     * Transforms the data.
     *
     * @param original Original data.
     * @param transformed Transformed data.
     * @return {@code 0} on successful transformation or byte buffer's capacity required to perform the transformation
     * when provided byte buffer's capacity in not enough.
     * @throws IgniteCheckedException when transformation is not possible/suitable.
     */
    public int transform(ByteBuffer original, ByteBuffer transformed) throws IgniteCheckedException;

    /**
     * Restores the data.
     *
     * @param transformed Transformed data.
     * @param restored Restored data.
     */
    public void restore(ByteBuffer transformed, ByteBuffer restored);

    /**
     * Direct byte buffer flag.
     *
     * @return True when direct byte buffers are required at {@link #transform(ByteBuffer, ByteBuffer)} and
     * {@link #restore(ByteBuffer, ByteBuffer)} methods.
     */
    public default boolean direct() {
        return false;
    }
}
