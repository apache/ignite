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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Cache objects transformer.
 */
@IgniteExperimental
public interface CacheObjectsTransformer extends Serializable {
    /**
     * @param original Original data.
     * @param transformed Transformed data.
     * @param overhead Additional space required to store transformed data.
     * @return {@code 0} on successful transformation or byte buffer's capacity required to perform the transformation
     * when provided byte buffer's capacity in not enough.
     */
    public int transform(ByteBuffer original, ByteBuffer transformed, int overhead) throws IgniteCheckedException;

    /**
     * @param transformed Transformed data.
     * @param restored Restored data.
     */
    public void restore(ByteBuffer transformed, ByteBuffer restored);

    /**
     * @return True when direct byte buffers are required.
     */
    public default boolean direct() {
        return false;
    }
}
