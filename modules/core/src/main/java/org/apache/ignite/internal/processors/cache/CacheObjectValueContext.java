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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.cache.transform.CacheObjectTransformerProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * Context to get value of cache object.
 */
public interface CacheObjectValueContext {
    /**
     * @return Copy on get flag.
     */
    public boolean copyOnGet();

    /**
     * @return {@code True} if should store unmarshalled value in cache.
     */
    public boolean storeValue();

    /**
     * @return {@code True} if deployment info should be associated with the objects of this cache.
     */
    public boolean addDeploymentInfo();

    /**
     * @return Binary enabled flag.
     */
    public boolean binaryEnabled();

    /**
     * @return Binary context.
     */
    public BinaryContext binaryContext();

    /**
     * Forces caller thread to wait for binary metadata write operation for given type ID.
     *
     * In case of in-memory mode this method becomes a No-op as no binary metadata is written to disk in this mode.
     *
     * @param typeId ID of binary type to wait for metadata write operation.
     */
    public void waitMetadataWriteIfNeeded(final int typeId);

    /**
     * @return User's class loader.
     */
    public @Nullable ClassLoader classLoader();

    /**
     * Gets distributed class loader.
     *
     * @return Cache class loader.
     */
    public ClassLoader globalLoader();

    /**
     * @param cls Class
     * @return Logger for class.
     */
    public IgniteLogger log(Class<?> cls);

    /**
     * @param val Value.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] marshal(Object val) throws IgniteCheckedException;

    /**
     * @param bytes Bytes.
     * @param clsLdr Class loader.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    public Object unmarshal(byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /** @return {@code true} if peer class loading is enabled, {@code false} otherwise. */
    public boolean isPeerClassLoadingEnabled();

    /**
     * Transforms bytes according to {@link CacheObjectTransformerProcessor} when specified.
     * @param bytes Given bytes.
     * @return Transformed bytes.
     */
    public default byte[] transformIfNecessary(byte[] bytes) {
        return transformIfNecessary(bytes, 0, bytes.length);
    }

    /**
     * Transforms bytes according to {@link CacheObjectTransformerProcessor} when specified.
     * @param bytes Given bytes.
     * @param offset Index to start from.
     * @param length Data length.
     * @return Transformed bytes.
     */
    public byte[] transformIfNecessary(byte[] bytes, int offset, int length);

    /**
     * Restores transformed bytes if necessary.
     * @param bytes Given bytes.
     * @return Restored bytes.
     */
    public byte[] restoreIfNecessary(byte[] bytes);
}
