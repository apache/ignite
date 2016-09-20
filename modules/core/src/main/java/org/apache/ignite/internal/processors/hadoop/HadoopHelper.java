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

package org.apache.ignite.internal.processors.hadoop;

import java.io.InputStream;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS utility processor adapter.
 */
public interface HadoopHelper {
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
     * @param cls Class name.
     * @return {@code true} If this is Hadoop class.
     */
    public boolean isHadoop(String cls);

    /**
     * Need to parse only Ignite Hadoop and IGFS classes.
     *
     * @param cls Class name.
     * @return {@code true} if we need to check this class.
     */
    public boolean isHadoopIgfs(String cls);

    /**
     * @param ldr Loader.
     * @param clsName Class.
     * @return Input stream.
     */
    @Nullable public InputStream loadClassBytes(ClassLoader ldr, String clsName);

    /**
     * Check whether class has external dependencies on Hadoop.
     *
     * @param clsName Class name.
     * @param parentClsLdr Parent class loader.
     * @return {@code True} if class has external dependencies.
     */
    public boolean hasExternalDependencies(String clsName, ClassLoader parentClsLdr);
}