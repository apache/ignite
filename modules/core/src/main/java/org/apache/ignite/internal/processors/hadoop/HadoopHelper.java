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

package org.apache.ignite.internal.processors.hadoop;

import java.io.InputStream;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS utility processor adapter.
 */
public interface HadoopHelper {
    /**
     * @return Whether this is no-op implementation.
     */
    public boolean isNoOp();

    /**
     * Get common Hadoop class loader.
     *
     * @return Common Hadoop class loader.
     */
    public HadoopClassLoader commonClassLoader();

    /**
     * Load special replacement and impersonate.
     *
     * @param in Input stream.
     * @param originalName Original class name.
     * @param replaceName Replacer class name.
     * @return Result.
     */
    public byte[] loadReplace(InputStream in, final String originalName, final String replaceName);

    /**
     * @param ldr Loader.
     * @param clsName Class.
     * @return Input stream.
     */
    @Nullable public InputStream loadClassBytes(ClassLoader ldr, String clsName);

    /**
     * Get work directory.
     *
     * @return Work directory.
     */
    public String workDirectory();

    /**
     * Close helper.
     */
    public void close();
}