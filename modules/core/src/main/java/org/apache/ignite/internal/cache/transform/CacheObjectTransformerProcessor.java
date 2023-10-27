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

package org.apache.ignite.internal.cache.transform;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.GridProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * Provides cache object's bytes transformation (eg. encryption, compression, etc).
 */
public interface CacheObjectTransformerProcessor extends GridProcessor {
    /**
     * Transforms the data.
     *
     * @param original Original data.
     * @return Transformed data (started with {@link GridBinaryMarshaller#TRANSFORMED} when restorable)
     * or {@code null} when transformation is not possible/suitable.
     */
    public @Nullable ByteBuffer transform(ByteBuffer original);

    /**
     * Restores the data.
     *
     * @param transformed Transformed data.
     * @return Restored data.
     */
    public ByteBuffer restore(ByteBuffer transformed);
}
