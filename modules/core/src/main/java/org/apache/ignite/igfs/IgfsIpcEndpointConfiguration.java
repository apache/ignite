/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

    /** Default threads count. */
    public static final int DFLT_THREAD_CNT = IgniteConfiguration.AVAILABLE_PROC_CNT;

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

    /** Thread count. */
    private int threadCnt = DFLT_THREAD_CNT;

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
     * @return {@code this} for chaining.
     */
    public IgfsIpcEndpointConfiguration setType(IgfsIpcEndpointType type) {
        this.type = type;

        return this;
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
     * @return {@code this} for chaining.
     */
    public IgfsIpcEndpointConfiguration setHost(String host) {
        this.host = host;

        return this;
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
     * @return {@code this} for chaining.
     */
    public IgfsIpcEndpointConfiguration setPort(int port) {
        this.port = port;

        return this;
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
     * @return {@code this} for chaining.
     */
    public IgfsIpcEndpointConfiguration setMemorySize(int memSize) {
        this.memSize = memSize;

        return this;
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
     * @return {@code this} for chaining.
     */
    public IgfsIpcEndpointConfiguration setTokenDirectoryPath(String tokenDirPath) {
        this.tokenDirPath = tokenDirPath;

        return this;
    }

    /**
     * Get number of threads used by this endpoint to process incoming requests.
     * <p>
     * Defaults to {@link #DFLT_THREAD_CNT}.
     *
     * @return Number of threads used by this endpoint to process incoming requests.
     */
    public int getThreadCount() {
        return threadCnt;
    }

    /**
     * Set number of threads used by this endpoint to process incoming requests.
     * <p>
     * See {@link #getThreadCount()} for more information.
     *
     * @param threadCnt Number of threads used by this endpoint to process incoming requests.
     * @return {@code this} for chaining.
     */
    public IgfsIpcEndpointConfiguration setThreadCount(int threadCnt) {
        this.threadCnt = threadCnt;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsIpcEndpointConfiguration.class, this);
    }
}