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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.IgniteSpi;

/**
 * SPI provides cache object's bytes transformation (eg. encryption, compression, etc).
 */
@IgniteExperimental
public interface CacheObjectTransformerSpi extends IgniteSpi {
    /** Additional space required to store the transformed data. */
    public int OVERHEAD = 2;

    /**
     * Transforms the data.
     *
     * @param bytes  Byte array contains the data.
     * @param offset Data offset.
     * @param length Data length.
     * @return Byte array contains the transformed data started with non-filled area with {@link #OVERHEAD} size.
     * @throws IgniteCheckedException when transformation is not possible/suitable.
     */
    public byte[] transform(byte[] bytes, int offset, int length) throws IgniteCheckedException;

    /**
     * Restores the data.
     *
     * @param bytes  Byte array contains the transformed data.
     * @param offset Data offset.
     * @param length Data length.
     * @return Byte array contains the restored data.
     */
    public byte[] restore(byte[] bytes, int offset, int length);
}
