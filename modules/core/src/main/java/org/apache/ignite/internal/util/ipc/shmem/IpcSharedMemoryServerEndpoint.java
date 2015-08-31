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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileLock;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcEndpointBindException;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Server shared memory IPC endpoint.
 */
public class IpcSharedMemoryServerEndpoint implements IpcServerEndpoint {
    /** IPC error message. */
    public static final String OUT_OF_RESOURCES_MSG = "Failed to allocate shared memory segment";

    /** Default endpoint port number. */
    public static final int DFLT_IPC_PORT = 10500;

    /** Default shared memory space in bytes. */
    public static final int DFLT_SPACE_SIZE = 256 * 1024;

    /**
     * Default token directory. Note that this path is relative to {@code IGNITE_HOME/work} folder
     * if {@code IGNITE_HOME} system or environment variable specified, otherwise it is relative to
     * {@code work} folder under system {@code java.io.tmpdir} folder.
     *
     * @see org.apache.ignite.configuration.IgniteConfiguration#getWorkDirectory()
     */
    public static final String DFLT_TOKEN_DIR_PATH = "ipc/shmem";

    /**
     * Shared memory token file name prefix.
     *
     * Token files are created and stored in the following manner: [tokDirPath]/[nodeId]-[current
     * PID]/gg-shmem-space-[auto_idx]-[other_party_pid]-[size]
     */
    public static final String TOKEN_FILE_NAME = "gg-shmem-space-";

    /** Default lock file name. */
    private static final String LOCK_FILE_NAME = "lock.file";

    /** GC frequency. */
    private static final long GC_FREQ = 10000;

    /** ID generator. */
    private static final AtomicLong tokIdxGen = new AtomicLong();

    /** Port to bind socket to. */
    private int port = DFLT_IPC_PORT;

    /** Prefix. */
    private String tokDirPath = DFLT_TOKEN_DIR_PATH;

    /** Space size. */
    private int size = DFLT_SPACE_SIZE;

    /** Server socket. */
    @GridToStringExclude
    private ServerSocket srvSock;

    /** Token directory. */
    private File tokDir;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Local node ID. */
    private UUID locNodeId;

    /** Grid name. */
    private String gridName;

    /** Flag allowing not to print out of resources warning. */
    private boolean omitOutOfResourcesWarn;

    /** GC worker. */
    private GridWorker gcWorker;

    /** Pid of the current process. */
    private int pid;

    /** Closed flag. */
    private volatile boolean closed;

    /** Spaces opened on with this endpoint. */
    private final Collection<IpcSharedMemoryClientEndpoint> endpoints =
        new GridConcurrentHashSet<>();

    /**
     * Use this constructor when dependencies could be injected
     * with {@link GridResourceProcessor#injectGeneric(Object)}.
     */
    public IpcSharedMemoryServerEndpoint() {
        // No-op.
    }

    /**
     * Constructor to set dependencies explicitly.
     *
     * @param log Log.
     * @param locNodeId Node id.
     * @param gridName Grid name.
     */
    public IpcSharedMemoryServerEndpoint(IgniteLogger log, UUID locNodeId, String gridName) {
        this.log = log;
        this.locNodeId = locNodeId;
        this.gridName = gridName;
    }

    /** @param omitOutOfResourcesWarn If {@code true}, out of resources warning will not be printed by server. */
    public void omitOutOfResourcesWarning(boolean omitOutOfResourcesWarn) {
        this.omitOutOfResourcesWarn = omitOutOfResourcesWarn;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        IpcSharedMemoryNativeLoader.load(log);

        pid = IpcSharedMemoryUtils.pid();

        if (pid == -1)
            throw new IpcEndpointBindException("Failed to get PID of the current process.");

        if (size <= 0)
            throw new IpcEndpointBindException("Space size should be positive: " + size);

        String tokDirPath = this.tokDirPath;

        if (F.isEmpty(tokDirPath))
            throw new IpcEndpointBindException("Token directory path is empty.");

        tokDirPath = tokDirPath + '/' + locNodeId.toString() + '-' + IpcSharedMemoryUtils.pid();

        tokDir = U.resolveWorkDirectory(tokDirPath, false);

        if (port <= 0 || port >= 0xffff)
            throw new IpcEndpointBindException("Port value is illegal: " + port);

        try {
            srvSock = new ServerSocket();

            // Always bind to loopback.
            srvSock.bind(new InetSocketAddress("127.0.0.1", port));
        }
        catch (IOException e) {
            // Although empty socket constructor never throws exception, close it just in case.
            U.closeQuiet(srvSock);

            throw new IpcEndpointBindException("Failed to bind shared memory IPC endpoint (is port already " +
                "in use?): " + port, e);
        }

        gcWorker = new GcWorker(gridName, "ipc-shmem-gc", log);

        new IgniteThread(gcWorker).start();

        if (log.isInfoEnabled())
            log.info("IPC shared memory server endpoint started [port=" + port +
                ", tokDir=" + tokDir.getAbsolutePath() + ']');
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ErrorNotRethrown")
    @Override public IpcEndpoint accept() throws IgniteCheckedException {
        while (!Thread.currentThread().isInterrupted()) {
            Socket sock = null;

            boolean accepted = false;

            try {
                sock = srvSock.accept();

                accepted = true;

                InputStream inputStream = sock.getInputStream();
                ObjectInputStream in = new ObjectInputStream(inputStream);

                ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

                IpcSharedMemorySpace inSpace = null;

                IpcSharedMemorySpace outSpace = null;

                boolean err = true;

                try {
                    IpcSharedMemoryInitRequest req = (IpcSharedMemoryInitRequest)in.readObject();

                    if (log.isDebugEnabled())
                        log.debug("Processing request: " + req);

                    IgnitePair<String> p = inOutToken(req.pid(), size);

                    String file1 = p.get1();
                    String file2 = p.get2();

                    assert file1 != null;
                    assert file2 != null;

                    // Create tokens.
                    new File(file1).createNewFile();
                    new File(file2).createNewFile();

                    if (log.isDebugEnabled())
                        log.debug("Created token files: " + p);

                    inSpace = new IpcSharedMemorySpace(
                        file1,
                        req.pid(),
                        pid,
                        size,
                        true,
                        log);

                    outSpace = new IpcSharedMemorySpace(
                        file2,
                        pid,
                        req.pid(),
                        size,
                        false,
                        log);

                    IpcSharedMemoryClientEndpoint ret = new IpcSharedMemoryClientEndpoint(inSpace, outSpace,
                        log);

                    out.writeObject(new IpcSharedMemoryInitResponse(file2, outSpace.sharedMemoryId(),
                        file1, inSpace.sharedMemoryId(), pid, size));

                    err = !in.readBoolean();

                    endpoints.add(ret);

                    return ret;
                }
                catch (UnsatisfiedLinkError e) {
                    throw IpcSharedMemoryUtils.linkError(e);
                }
                catch (IOException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to process incoming connection " +
                            "(was connection closed by another party):" + e.getMessage());
                }
                catch (ClassNotFoundException e) {
                    U.error(log, "Failed to process incoming connection.", e);
                }
                catch (ClassCastException e) {
                    String msg = "Failed to process incoming connection (most probably, shared memory " +
                        "rest endpoint has been configured by mistake).";

                    LT.warn(log, null, msg);

                    sendErrorResponse(out, e);
                }
                catch (IpcOutOfSystemResourcesException e) {
                    if (!omitOutOfResourcesWarn)
                        LT.warn(log, null, OUT_OF_RESOURCES_MSG);

                    sendErrorResponse(out, e);
                }
                catch (IgniteCheckedException e) {
                    LT.error(log, e, "Failed to process incoming shared memory connection.");

                    sendErrorResponse(out, e);
                }
                finally {
                    // Exception has been thrown, need to free system resources.
                    if (err) {
                        if (inSpace != null)
                            inSpace.forceClose();

                        // Safety.
                        if (outSpace != null)
                            outSpace.forceClose();
                    }
                }
            }
            catch (IOException e) {
                if (!Thread.currentThread().isInterrupted() && !accepted)
                    throw new IgniteCheckedException("Failed to accept incoming connection.", e);

                if (!closed)
                    LT.error(log, null, "Failed to process incoming shared memory connection: " + e.getMessage());
            }
            finally {
                U.closeQuiet(sock);
            }
        } // while

        throw new IgniteInterruptedCheckedException("Socket accept was interrupted.");
    }

    /**
     * Injects resources.
     *
     * @param ignite Ignite
     */
    @IgniteInstanceResource
    private void injectResources(Ignite ignite){
        if (ignite != null) {
            // Inject resources.
            gridName = ignite.name();
            locNodeId = ignite.configuration().getNodeId();
        }
        else {
            // Cleanup resources.
            gridName = null;
            locNodeId = null;
        }
    }

    /**
     * @param out Output stream.
     * @param err Error cause.
     */
    private void sendErrorResponse(ObjectOutput out, Exception err) {
        try {
            out.writeObject(new IpcSharedMemoryInitResponse(err));
        }
        catch (IOException e) {
            U.error(log, "Failed to send error response to client.", e);
        }
    }

    /**
     * @param pid PID of the other party.
     * @param size Size of the space.
     * @return Token pair.
     */
    private IgnitePair<String> inOutToken(int pid, int size) {
        while (true) {
            long idx = tokIdxGen.get();

            if (tokIdxGen.compareAndSet(idx, idx + 2))
                return F.pair(new File(tokDir, TOKEN_FILE_NAME + idx + "-" + pid + "-" + size).getAbsolutePath(),
                    new File(tokDir, TOKEN_FILE_NAME + (idx + 1) + "-" + pid + "-" + size).getAbsolutePath());
        }
    }

    /** {@inheritDoc} */
    @Override public int getPort() {
        return port;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String getHost() {
        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code false} as shared memory endpoints can not be used for management.
     */
    @Override public boolean isManagement() {
        return false;
    }

    /**
     * Sets port endpoint will be bound to.
     *
     * @param port Port number.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets token directory path.
     *
     * @return Token directory path.
     */
    public String getTokenDirectoryPath() {
        return tokDirPath;
    }

    /**
     * Sets token directory path.
     *
     * @param tokDirPath Token directory path.
     */
    public void setTokenDirectoryPath(String tokDirPath) {
        this.tokDirPath = tokDirPath;
    }

    /**
     * Gets size of shared memory spaces that are created by the endpoint.
     *
     * @return Size of shared memory space.
     */
    public int getSize() {
        return size;
    }

    /**
     * Sets size of shared memory spaces that are created by the endpoint.
     *
     * @param size Size of shared memory space.
     */
    public void setSize(int size) {
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        closed = true;

        U.closeQuiet(srvSock);

        if (gcWorker != null) {
            U.cancel(gcWorker);

            // This method may be called from already interrupted thread.
            // Need to ensure cleaning on close.
            boolean interrupted = Thread.interrupted();

            try {
                U.join(gcWorker);
            }
            catch (IgniteInterruptedCheckedException e) {
                U.warn(log, "Interrupted when stopping GC worker.", e);
            }
            finally {
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IpcSharedMemoryServerEndpoint.class, this);
    }

    /**
     * Sets configuration properties from the map.
     *
     * @param endpointCfg Map of properties.
     * @throws IgniteCheckedException If invalid property name or value.
     */
    public void setupConfiguration(Map<String, String> endpointCfg) throws IgniteCheckedException {
        for (Map.Entry<String,String> e : endpointCfg.entrySet()) {
            try {
                switch (e.getKey()) {
                    case "type":
                    case "host":
                    case "management":
                        //Ignore these properties
                        break;

                    case "port":
                        setPort(Integer.parseInt(e.getValue()));
                        break;

                    case "size":
                        setSize(Integer.parseInt(e.getValue()));
                        break;

                    case "tokenDirectoryPath":
                        setTokenDirectoryPath(e.getValue());
                        break;

                    default:
                        throw new IgniteCheckedException("Invalid property '" + e.getKey() + "' of " + getClass().getSimpleName());
                }
            }
            catch (Throwable t) {
                if (t instanceof IgniteCheckedException || t instanceof Error)
                    throw t;

                throw new IgniteCheckedException("Invalid value '" + e.getValue() + "' of the property '" + e.getKey() + "' in " +
                        getClass().getSimpleName(), t);
            }
        }
    }

    /**
     *
     */
    private class GcWorker extends GridWorker {
        /**
         * @param gridName Grid name.
         * @param name Name.
         * @param log Log.
         */
        protected GcWorker(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            if (log.isDebugEnabled())
                log.debug("GC worker started.");

            File workTokDir = tokDir.getParentFile();

            assert workTokDir != null;

            boolean lastRunNeeded = true;

            while (true) {
                try {
                    // Sleep only if not cancelled.
                    if (lastRunNeeded)
                        Thread.sleep(GC_FREQ);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                if (log.isDebugEnabled())
                    log.debug("Starting GC iteration.");

                cleanupResources(workTokDir);

                // Process spaces created by this endpoint.
                if (log.isDebugEnabled())
                    log.debug("Processing local spaces.");

                for (IpcSharedMemoryClientEndpoint e : endpoints) {
                    if (log.isDebugEnabled())
                        log.debug("Processing endpoint: " + e);

                    if (!e.checkOtherPartyAlive()) {
                        endpoints.remove(e);

                        if (log.isDebugEnabled())
                            log.debug("Removed endpoint: " + e);
                    }
                }

                if (isCancelled()) {
                    if (lastRunNeeded) {
                        lastRunNeeded = false;

                        // Clear interrupted status.
                        Thread.interrupted();
                    }
                    else {
                        Thread.currentThread().interrupt();

                        break;
                    }
                }
            }
        }

        /**
         * @param workTokDir Token directory (common for multiple nodes).
         */
        private void cleanupResources(File workTokDir) {
            RandomAccessFile lockFile = null;

            FileLock lock = null;

            try {
                lockFile = new RandomAccessFile(new File(workTokDir, LOCK_FILE_NAME), "rw");

                lock = lockFile.getChannel().lock();

                if (lock != null)
                    processTokenDirectory(workTokDir);
                else if (log.isDebugEnabled())
                    log.debug("Token directory is being processed concurrently: " + workTokDir.getAbsolutePath());
            }
            catch (OverlappingFileLockException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Token directory is being processed concurrently: " + workTokDir.getAbsolutePath());
            }
            catch (FileLockInterruptionException ignored) {
                Thread.currentThread().interrupt();
            }
            catch (IOException e) {
                U.error(log, "Failed to process directory: " + workTokDir.getAbsolutePath(), e);
            }
            finally {
                U.releaseQuiet(lock);
                U.closeQuiet(lockFile);
            }
        }

        /**
         * @param workTokDir Token directory (common for multiple nodes).
         */
        private void processTokenDirectory(File workTokDir) {
            for (File f : workTokDir.listFiles()) {
                if (!f.isDirectory()) {
                    if (!f.getName().equals(LOCK_FILE_NAME)) {
                        if (log.isDebugEnabled())
                            log.debug("Unexpected file: " + f.getName());
                    }

                    continue;
                }

                if (f.equals(tokDir)) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping own token directory: " + tokDir.getName());

                    continue;
                }

                String name = f.getName();

                int pid;

                try {
                    pid = Integer.parseInt(name.substring(name.lastIndexOf('-') + 1));
                }
                catch (NumberFormatException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to parse file name: " + name);

                    continue;
                }

                // Is process alive?
                if (IpcSharedMemoryUtils.alive(pid)) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping alive node: " + pid);

                    continue;
                }

                if (log.isDebugEnabled())
                    log.debug("Possibly stale token folder: " + f);

                // Process each token under stale token folder.
                File[] shmemToks = f.listFiles();

                if (shmemToks == null)
                    // Although this is strange, but is reproducible sometimes on linux.
                    return;

                int rmvCnt = 0;

                try {
                    for (File f0 : shmemToks) {
                        if (log.isDebugEnabled())
                            log.debug("Processing token file: " + f0.getName());

                        if (f0.isDirectory()) {
                            if (log.isDebugEnabled())
                                log.debug("Unexpected directory: " + f0.getName());
                        }

                        // Token file format: gg-shmem-space-[auto_idx]-[other_party_pid]-[size]
                        String[] toks = f0.getName().split("-");

                        if (toks.length != 6) {
                            if (log.isDebugEnabled())
                                log.debug("Unrecognized token file: " + f0.getName());

                            continue;
                        }

                        int pid0;
                        int size;

                        try {
                            pid0 = Integer.parseInt(toks[4]);
                            size = Integer.parseInt(toks[5]);
                        }
                        catch (NumberFormatException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to parse file name: " + name);

                            continue;
                        }

                        if (IpcSharedMemoryUtils.alive(pid0)) {
                            if (log.isDebugEnabled())
                                log.debug("Skipping alive process: " + pid0);

                            continue;
                        }

                        if (log.isDebugEnabled())
                            log.debug("Possibly stale token file: " + f0);

                        IpcSharedMemoryUtils.freeSystemResources(f0.getAbsolutePath(), size);

                        if (f0.delete()) {
                            if (log.isDebugEnabled())
                                log.debug("Deleted file: " + f0.getName());

                            rmvCnt++;
                        }
                        else if (!f0.exists()) {
                            if (log.isDebugEnabled())
                                log.debug("File has been concurrently deleted: " + f0.getName());

                            rmvCnt++;
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Failed to delete file: " + f0.getName());
                    }
                }
                finally {
                    // Assuming that no new files can appear, since
                    if (rmvCnt == shmemToks.length) {
                        U.delete(f);

                        if (log.isDebugEnabled())
                            log.debug("Deleted empty token directory: " + f.getName());
                    }
                }
            }
        }
    }
}