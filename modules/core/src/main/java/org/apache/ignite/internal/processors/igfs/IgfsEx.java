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

package org.apache.ignite.internal.processors.igfs;

import java.net.URI;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Internal API extension for {@link org.apache.ignite.IgniteFileSystem}.
 */
public interface IgfsEx extends IgniteFileSystem {
    /**
     * Stops IGFS cleaning all used resources.
     *
     * @param cancel Cancellation flag.
     */
    public void stop(boolean cancel);

    /**
     * @return IGFS context.
     */
    public IgfsContext context();

    /**
     * Gets global space counters.
     *
     * @return Tuple in which first component is used space on all nodes,
     *      second is available space on all nodes.
     * @throws IgniteCheckedException If task execution failed.
     */
    public IgfsStatus globalSpace() throws IgniteCheckedException;

    /**
     * Enables, disables or clears sampling flag.
     *
     * @param val {@code True} to turn on sampling, {@code false} to turn it off, {@code null} to clear sampling state.
     * @throws IgniteCheckedException If failed.
     */
    public void globalSampling(@Nullable Boolean val) throws IgniteCheckedException;

    /**
     * Get sampling state.
     *
     * @return {@code True} in case sampling is enabled, {@code false} otherwise, or {@code null} in case sampling
     * flag is not set.
     */
    @Nullable public Boolean globalSampling();

    /**
     * Gets group block size, i.e. block size multiplied by group size in affinity mapper.
     *
     * @return Group block size.
     */
    public long groupBlockSize();

    /**
     * Gets client file system log directory.
     *
     * @return Client file system log directory or {@code null} in case no client connections have been created yet.
     */
    @Nullable public String clientLogDirectory();

    /**
     * Sets client file system log directory.
     *
     * @param logDir Client file system log directory.
     */
    public void clientLogDirectory(String logDir);

    /**
     * Whether this path is excluded from evictions.
     *
     * @param path Path.
     * @param primary Whether the mode is PRIMARY.
     * @return {@code True} if path is excluded from evictions.
     */
    public boolean evictExclude(IgfsPath path, boolean primary);

    /**
     * Get next affinity key.
     *
     * @return Next affinity key.
     */
    public IgniteUuid nextAffinityKey();

    /**
     * Check whether the given path is proxy path.
     *
     * @param path Path.
     * @return {@code True} if proxy.
     */
    public boolean isProxy(URI path);

    /**
     * Return the given IGFS as a secondary file system.
     *
     * @return Secondary file system wrapper.
     */
    public IgfsSecondaryFileSystem asSecondary();

    /**
     * Await for any pending finished writes on the children paths.
     *
     * @param paths Paths to check.
     */
    public void await(IgfsPath... paths);
}
