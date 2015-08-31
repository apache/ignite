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
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Internal API extension for {@link org.apache.ignite.IgniteFileSystem}.
 */
public interface IgfsEx extends IgniteFileSystem {
    /** File property: user name. */
    public static final String PROP_USER_NAME = "usrName";

    /** File property: group name. */
    public static final String PROP_GROUP_NAME = "grpName";

    /** File property: permission. */
    public static final String PROP_PERMISSION = "permission";

    /** File property: prefer writes to local node. */
    public static final String PROP_PREFER_LOCAL_WRITES = "locWrite";

    /** Property name for path to Hadoop configuration. */
    public static final String SECONDARY_FS_CONFIG_PATH = "SECONDARY_FS_CONFIG_PATH";

    /** Property name for URI of file system. */
    public static final String SECONDARY_FS_URI = "SECONDARY_FS_URI";

    /** Property name for default user name of file system.
     * NOTE: for secondary file system this is just a default user name, which is used
     * when the 2ndary filesystem is used outside of any user context.
     * If another user name is set in the context, 2ndary file system will work on behalf
     * of that user, which is different from the default. */
     public static final String SECONDARY_FS_USER_NAME = "SECONDARY_FS_USER_NAME";

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
     * Get handshake message.
     *
     * @return Handshake message.
     */
    public IgfsPaths proxyPaths();

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path, int bufSize, int seqReadsBeforePrefetch)
        throws IgniteException;

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path) throws IgniteException;

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path, int bufSize) throws IgniteException;

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
     * Get local metrics.
     *
     * @return Local metrics.
     */
    public IgfsLocalMetrics localMetrics();

    /**
     * Gets group block size, i.e. block size multiplied by group size in affinity mapper.
     *
     * @return Group block size.
     */
    public long groupBlockSize();

    /**
     * Asynchronously await for all entries existing in trash to be removed.
     *
     * @return Future which will be completed when all entries existed in trash by the time of invocation are removed.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteInternalFuture<?> awaitDeletesAsync() throws IgniteCheckedException;

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
}