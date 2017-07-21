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
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * IPC endpoint based on shared memory space.
 */
public class IpcSharedMemoryClientEndpoint implements IpcEndpoint {
    /** In space. */
    private final IpcSharedMemorySpace inSpace;

    /** Out space. */
    private final IpcSharedMemorySpace outSpace;

    /** In space. */
    private final IpcSharedMemoryInputStream in;

    /** Out space. */
    private final IpcSharedMemoryOutputStream out;

    /** */
    private boolean checkIn = true;

    /** */
    private boolean checkOut = true;

    /** */
    private final Thread checker;

    /** */
    private final IgniteLogger log;

    /**
     * Creates connected client IPC endpoint.
     *
     * @param inSpace In space.
     * @param outSpace Out space.
     * @param parent Parent logger.
     */
    public IpcSharedMemoryClientEndpoint(IpcSharedMemorySpace inSpace,
        IpcSharedMemorySpace outSpace,
        IgniteLogger parent) {
        assert inSpace != null;
        assert outSpace != null;

        log = parent.getLogger(IpcSharedMemoryClientEndpoint.class);

        this.inSpace = inSpace;
        this.outSpace = outSpace;

        in = new IpcSharedMemoryInputStream(inSpace);
        out = new IpcSharedMemoryOutputStream(outSpace);

        checker = null;
    }

    /**
     * Creates and connects client IPC endpoint and starts background checker thread to avoid deadlocks on other party
     * crash. Waits until port became available.
     *
     * @param port Port server endpoint bound to.
     * @param parent Parent logger.
     * @throws IgniteCheckedException If connection fails.
     */
    public IpcSharedMemoryClientEndpoint(int port, IgniteLogger parent) throws IgniteCheckedException {
        this(port, 0, parent);
    }

    /**
     * Creates and connects client IPC endpoint and starts background checker thread to avoid deadlocks on other party
     * crash.
     *
     * @param port Port server endpoint bound to.
     * @param timeout Connection timeout.
     * @param parent Parent logger.
     * @throws IgniteCheckedException If connection fails.
     */
    @SuppressWarnings({"CallToThreadStartDuringObjectConstruction", "ErrorNotRethrown"})
    public IpcSharedMemoryClientEndpoint(int port, int timeout, IgniteLogger parent) throws IgniteCheckedException {
        assert port > 0;
        assert port < 0xffff;

        log = parent.getLogger(IpcSharedMemoryClientEndpoint.class);

        IpcSharedMemorySpace inSpace = null;
        IpcSharedMemorySpace outSpace = null;

        Socket sock = new Socket();

        Exception err = null;
        boolean clear = true;

        try {
            IpcSharedMemoryNativeLoader.load(log);

            sock.connect(new InetSocketAddress("127.0.0.1", port), timeout);

            // Send request.
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            int pid = IpcSharedMemoryUtils.pid();

            out.writeObject(new IpcSharedMemoryInitRequest(pid));

            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            IpcSharedMemoryInitResponse res = (IpcSharedMemoryInitResponse)in.readObject();

            err = res.error();

            if (err == null) {
                String inTokFileName = res.inTokenFileName();

                assert inTokFileName != null;

                inSpace = new IpcSharedMemorySpace(inTokFileName, res.pid(), pid, res.size(), true,
                    res.inSharedMemoryId(), log);

                String outTokFileName = res.outTokenFileName();

                assert outTokFileName != null;

                outSpace = new IpcSharedMemorySpace(outTokFileName, pid, res.pid(), res.size(), false,
                    res.outSharedMemoryId(), log);

                // This is success ACK.
                out.writeBoolean(true);

                out.flush();

                clear = false;
            }
        }
        catch (UnsatisfiedLinkError e) {
            throw IpcSharedMemoryUtils.linkError(e);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to connect shared memory endpoint to port " +
                "(is shared memory server endpoint up and running?): " + port, e);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            U.closeQuiet(sock);

            if (clear) {
                if (inSpace != null)
                    inSpace.forceClose();

                if (outSpace != null)
                    outSpace.forceClose();
            }
        }

        if (err != null) // Error response.
            throw new IgniteCheckedException(err);

        this.inSpace = inSpace;
        this.outSpace = outSpace;

        in = new IpcSharedMemoryInputStream(inSpace);
        out = new IpcSharedMemoryOutputStream(outSpace);

        checker = new Thread(new AliveChecker());

        // Required for Hadoop 2.x
        checker.setDaemon(true);

        checker.start();
    }

    /** {@inheritDoc} */
    @Override public InputStream inputStream() {
        return in;
    }

    /** {@inheritDoc} */
    @Override public OutputStream outputStream() {
        return out;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(in);
        U.closeQuiet(out);

        stopChecker();
    }

    /**
     * Forcibly closes spaces and frees all system resources. <p> This method should be called with caution as it may
     * result to the other-party process crash. It is intended to call when there was an IO error during handshake and
     * other party has not yet attached to the space.
     */
    public void forceClose() {
        in.forceClose();
        out.forceClose();

        stopChecker();
    }

    /**
     *
     */
    private void stopChecker() {
        if (checker != null) {
            checker.interrupt();

            try {
                checker.join();
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** @return {@code True} if other party is alive and new invocation of this method needed. */
    boolean checkOtherPartyAlive() {
        if (checkIn) {
            File tokFile = new File(inSpace.tokenFileName());

            if (!tokFile.exists())
                checkIn = false;
        }

        if (checkOut) {
            File tokFile = new File(outSpace.tokenFileName());

            if (!tokFile.exists())
                checkOut = false;
        }

        if (!checkIn && !checkOut)
            return false;

        if (!IpcSharedMemoryUtils.alive(inSpace.otherPartyPid())) {
            U.warn(log, "Remote process is considered to be dead (shared memory space will be forcibly closed): " +
                inSpace.otherPartyPid());

            closeSpace(inSpace);
            closeSpace(outSpace);

            return false;
        }

        // Need to call this method again after timeout.
        return true;
    }

    /**
     * This method is intended for test purposes only.
     *
     * @return In space.
     */
    IpcSharedMemorySpace inSpace() {
        return inSpace;
    }

    /**
     * This method is intended for test purposes only.
     *
     * @return Out space.
     */
    IpcSharedMemorySpace outSpace() {
        return outSpace;
    }

    /** @param space Space to close. */
    private void closeSpace(IpcSharedMemorySpace space) {
        assert space != null;

        space.forceClose();

        File tokFile = new File(space.tokenFileName());

        // Space is not usable at this point and all local threads
        // are guaranteed to leave its methods (other party is not alive).
        // So, we can cleanup resources without additional synchronization.
        IpcSharedMemoryUtils.freeSystemResources(tokFile.getAbsolutePath(), space.size());

        tokFile.delete();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IpcSharedMemoryClientEndpoint.class, this);
    }

    /**
     *
     */
    private class AliveChecker implements Runnable {
        /** Check frequency. */
        private static final long CHECK_FREQ = 10000;

        /** {@inheritDoc} */
        @SuppressWarnings("BusyWait")
        @Override public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(CHECK_FREQ);
                }
                catch (InterruptedException ignored) {
                    return;
                }

                if (!checkOtherPartyAlive())
                    // No need to check any more.
                    return;
            }
        }
    }
}