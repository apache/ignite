/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;

import java.io.*;
import java.net.*;

/**
 * IPC endpoint based on shared memory space.
 */
public class GridIpcSharedMemoryClientEndpoint implements GridIpcEndpoint {
    /** In space. */
    private final GridIpcSharedMemorySpace inSpace;

    /** Out space. */
    private final GridIpcSharedMemorySpace outSpace;

    /** In space. */
    private final GridIpcSharedMemoryInputStream in;

    /** Out space. */
    private final GridIpcSharedMemoryOutputStream out;

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
    public GridIpcSharedMemoryClientEndpoint(GridIpcSharedMemorySpace inSpace, GridIpcSharedMemorySpace outSpace,
        IgniteLogger parent) {
        assert inSpace != null;
        assert outSpace != null;

        log = parent.getLogger(GridIpcSharedMemoryClientEndpoint.class);

        this.inSpace = inSpace;
        this.outSpace = outSpace;

        in = new GridIpcSharedMemoryInputStream(inSpace);
        out = new GridIpcSharedMemoryOutputStream(outSpace);

        checker = null;
    }

    /**
     * Creates and connects client IPC endpoint and starts background checker thread to avoid deadlocks on other party
     * crash. Waits until port became available.
     *
     * @param port Port server endpoint bound to.
     * @param parent Parent logger.
     * @throws GridException If connection fails.
     */
    public GridIpcSharedMemoryClientEndpoint(int port, IgniteLogger parent) throws GridException {
        this(port, 0, parent);
    }

    /**
     * Creates and connects client IPC endpoint and starts background checker thread to avoid deadlocks on other party
     * crash.
     *
     * @param port Port server endpoint bound to.
     * @param timeout Connection timeout.
     * @param parent Parent logger.
     * @throws GridException If connection fails.
     */
    @SuppressWarnings({"CallToThreadStartDuringObjectConstruction", "ErrorNotRethrown"})
    public GridIpcSharedMemoryClientEndpoint(int port, int timeout, IgniteLogger parent) throws GridException {
        assert port > 0;
        assert port < 0xffff;

        log = parent.getLogger(GridIpcSharedMemoryClientEndpoint.class);

        GridIpcSharedMemorySpace inSpace = null;
        GridIpcSharedMemorySpace outSpace = null;

        Socket sock = new Socket();

        Exception err = null;
        boolean clear = true;

        try {
            GridIpcSharedMemoryNativeLoader.load();

            sock.connect(new InetSocketAddress("127.0.0.1", port), timeout);

            // Send request.
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());

            int pid = GridIpcSharedMemoryUtils.pid();

            out.writeObject(new GridIpcSharedMemoryInitRequest(pid));

            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());

            GridIpcSharedMemoryInitResponse res = (GridIpcSharedMemoryInitResponse)in.readObject();

            err = res.error();

            if (err == null) {
                String inTokFileName = res.inTokenFileName();

                assert inTokFileName != null;

                inSpace = new GridIpcSharedMemorySpace(inTokFileName, res.pid(), pid, res.size(), true,
                    res.inSharedMemoryId(), log);

                String outTokFileName = res.outTokenFileName();

                assert outTokFileName != null;

                outSpace = new GridIpcSharedMemorySpace(outTokFileName, pid, res.pid(), res.size(), false,
                    res.outSharedMemoryId(), log);

                // This is success ACK.
                out.writeBoolean(true);

                out.flush();

                clear = false;
            }
        }
        catch (UnsatisfiedLinkError e) {
            throw GridIpcSharedMemoryUtils.linkError(e);
        }
        catch (IOException e) {
            throw new GridException("Failed to connect shared memory endpoint to port " +
                "(is shared memory server endpoint up and running?): " + port, e);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw new GridException(e);
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
            throw new GridException(err);

        this.inSpace = inSpace;
        this.outSpace = outSpace;

        in = new GridIpcSharedMemoryInputStream(inSpace);
        out = new GridIpcSharedMemoryOutputStream(outSpace);

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

        if (!GridIpcSharedMemoryUtils.alive(inSpace.otherPartyPid())) {
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
    GridIpcSharedMemorySpace inSpace() {
        return inSpace;
    }

    /**
     * This method is intended for test purposes only.
     *
     * @return Out space.
     */
    GridIpcSharedMemorySpace outSpace() {
        return outSpace;
    }

    /** @param space Space to close. */
    private void closeSpace(GridIpcSharedMemorySpace space) {
        assert space != null;

        space.forceClose();

        File tokFile = new File(space.tokenFileName());

        // Space is not usable at this point and all local threads
        // are guaranteed to leave its methods (other party is not alive).
        // So, we can cleanup resources without additional synchronization.
        GridIpcSharedMemoryUtils.freeSystemResources(tokFile.getAbsolutePath(), space.size());

        tokFile.delete();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIpcSharedMemoryClientEndpoint.class, this);
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
