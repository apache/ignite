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

package org.apache.ignite.igfs;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.igfs.IgfsIpcEndpointType.SHMEM;
import static org.apache.ignite.igfs.IgfsIpcEndpointType.TCP;

/**
 * IGFS IPC endpoint configuration.
 */
public class IgfsIpcEndpointConfiguration {
    /** Default endpoint type is TCP. */
    public static IgfsIpcEndpointType DFLT_TYPE = U.hasSharedMemory() ? SHMEM : TCP;

    /** Default host. */
    public static String DFLT_HOST = "127.0.0.1";

    /** Default port. */
    public static int DFLT_PORT = 10500;

    /** Default shared memory space in bytes. */
    public static final int DFLT_MEM_SIZE = 256 * 1024;

    /**
     * Default token directory. Note that this path is relative to {@code IGNITE_HOME/work} folder
     * if {@code IGNITE_HOME} system or environment variable specified, otherwise it is relative to
     * {@code work} folder under system {@code java.io.tmpdir} folder.
     *
     * @see IgniteConfiguration#getWorkDirectory()
     */
    public static final String DFLT_TOKEN_DIR_PATH = "ipc/shmem";

    /** Endpoint type. */
    private IgfsIpcEndpointType type = DFLT_TYPE;

    /** Host. */
    private String host = DFLT_HOST;

    /** Port. */
    private int port = DFLT_PORT;

    /** Space size. */
    private int memSize = DFLT_MEM_SIZE;

    /** Token directory path. */
    private String tokenDirPath = DFLT_TOKEN_DIR_PATH;

    /**
     * Default constructor.
     */
    public IgfsIpcEndpointConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Configuration to copy.
     */
    public IgfsIpcEndpointConfiguration(IgfsIpcEndpointConfiguration cfg) {
        type = cfg.getType();
        host = cfg.getHost();
        port = cfg.getPort();
        memSize = cfg.getMemorySize();
        tokenDirPath = cfg.getTokenDirectoryPath();
    }

    /**
     * Gets endpoint type. There are two endpoints types: {@code SHMEM} working over shared memory, and {@code TCP}
     * working over sockets.
     * <p>
     * Shared memory is recommended approach for Linux-based systems. For Windows TCP is the only available option.
     * <p>
     * Defaults to {@link #DFLT_TYPE}.
     *
     * @return Endpoint type.
     */
    public IgfsIpcEndpointType getType() {
        return type;
    }

    /**
     * Sets endpoint type. There are two endpoints types: {@link IgfsIpcEndpointType#SHMEM} working over shared memory,
     * and {@link IgfsIpcEndpointType#TCP} working over sockets.
     * <p>
     * Shared memory is recommended approach for Linux-based systems. For Windows TCP is the only available option.
     * <p>
     * Defaults to {@link #DFLT_TYPE}.
     *
     * @param type Endpoint type.
     */
    public void setType(IgfsIpcEndpointType type) {
        this.type = type;
    }

    /**
     * Gets the host endpoint is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#TCP} endpoint this is the network interface server socket is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#SHMEM} endpoint socket connection is needed only to perform an initial handshake.
     * All further communication is performed over shared memory. Therefore, for {@code SHMEM} this value is ignored
     * and socket will be always bound to {@link #DFLT_HOST}.
     * <p>
     * Defaults to {@link #DFLT_HOST}.
     *
     * @return Host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host endpoint is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#TCP} endpoint this is the network interface server socket is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#SHMEM} endpoint socket connection is needed only to perform an initial handshake.
     * All further communication is performed over shared memory. Therefore, for {@code SHMEM} this value is ignored
     * and socket will be always bound to {@link #DFLT_HOST}.
     * <p>
     * Defaults to {@link #DFLT_HOST}.
     *
     * @param host Host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets the port endpoint is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#TCP} endpoint this is the port server socket is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#SHMEM} endpoint socket connection is needed only to perform an initial handshake.
     * All further communication is performed over shared memory.
     * <p>
     * Defaults to {@link #DFLT_PORT}.
     *
     * @return Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port endpoint is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#TCP} endpoint this is the port server socket is bound to.
     * <p>
     * For {@link IgfsIpcEndpointType#SHMEM} endpoint socket connection is needed only to perform an initial handshake.
     * All further communication is performed over shared memory.
     * <p>
     * Defaults to {@link #DFLT_PORT}.
     *
     * @param port Port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets shared memory size in bytes allocated for endpoint communication.
     * <p>
     * Ignored for {@link IgfsIpcEndpointType#TCP} endpoint.
     * <p>
     * Defaults to {@link #DFLT_MEM_SIZE}.
     *
     * @return Shared memory size.
     */
    public int getMemorySize() {
        return memSize;
    }

    /**
     * Sets shared memory size in bytes allocated for endpoint communication.
     * <p>
     * Ignored for {@link IgfsIpcEndpointType#TCP} endpoint.
     * <p>
     * Defaults to {@link #DFLT_MEM_SIZE}.
     *
     * @param memSize Shared memory size.
     */
    public void setMemorySize(int memSize) {
        this.memSize = memSize;
    }

    /**
     * Gets directory where shared memory tokens are stored.
     * <p>
     * Note that this path is relative to {@code IGNITE_HOME/work} folder if {@code IGNITE_HOME} system or environment
     * variable specified, otherwise it is relative to {@code work} folder under system {@code java.io.tmpdir} folder.
     * <p>
     * Ignored for {@link IgfsIpcEndpointType#TCP} endpoint.
     * <p>
     * Defaults to {@link #DFLT_TOKEN_DIR_PATH}.
     *
     * @return Directory where shared memory tokens are stored.
     */
    public String getTokenDirectoryPath() {
        return tokenDirPath;
    }

    /**
     * Sets directory where shared memory tokens are stored.
     * <p>
     * Note that this path is relative to {@code IGNITE_HOME/work} folder if {@code IGNITE_HOME} system or environment
     * variable specified, otherwise it is relative to {@code work} folder under system {@code java.io.tmpdir} folder.
     * <p>
     * Ignored for {@link IgfsIpcEndpointType#TCP} endpoint.
     * <p>
     * Defaults to {@link #DFLT_TOKEN_DIR_PATH}.
     *
     * @param tokenDirPath Directory where shared memory tokens are stored.
     */
    public void setTokenDirectoryPath(String tokenDirPath) {
        this.tokenDirPath = tokenDirPath;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsIpcEndpointConfiguration.class, this);
    }
}