/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.lang.management.*;
import java.nio.*;
import java.util.*;

/**
 * NOTE: Native library should be loaded, before methods of this class are called. Native library is loaded with: {@link
 * GridIpcSharedMemoryNativeLoader#load()}.
 */
public class GridIpcSharedMemoryUtils {
    /**
     * Allocates shared memory segment and semaphores for IPC exchange.
     *
     * @param tokFileName OS token file name.
     * @param size Memory space size in bytes.
     * @param debug {@code True} to output debug to stdout (will set global flag).
     * @return Shared memory pointer.
     * @throws IgniteCheckedException If failed.
     */
    static native long allocateSystemResources(String tokFileName, int size, boolean debug)
        throws IgniteCheckedException;

    /**
     * Attaches to previously allocated shared memory segment.
     *
     * @param shmemId OS shared memory segment ID.
     * @param debug {@code True} to output debug to stdout (will set global flag).
     * @return Shared memory pointer.
     * @throws IgniteCheckedException If failed.
     */
    static native long attach(int shmemId, boolean debug) throws IgniteCheckedException;

    /**
     * Stops IPC communication. Call {@link #freeSystemResources(String, long, boolean)} after this call.
     *
     * @param shmemPtr Shared memory pointer.
     */
    static native void ipcClose(long shmemPtr);

    /**
     * Frees system resources.
     *
     * @param tokFileName Token file name.
     * @param shmemPtr Shared memory pointer
     * @param force {@code True} to force close.
     */
    static native void freeSystemResources(String tokFileName, long shmemPtr, boolean force);

    /**
     * Frees system resources.
     *
     * @param tokFileName Token file name.
     * @param size Size.
     */
    static native void freeSystemResources(String tokFileName, int size);

    /**
     * @param shMemPtr Shared memory pointer.
     * @param dest Destination buffer.
     * @param dOff Destination offset.
     * @param size Size.
     * @param timeout Operation timeout.
     * @return Read bytes count.
     * @throws IgniteCheckedException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    static native long readSharedMemory(long shMemPtr, byte dest[], long dOff, long size, long timeout)
        throws IgniteCheckedException, GridIpcSharedMemoryOperationTimedoutException;

    /**
     * @param shmemPtr Shared memory pointer.
     * @return Unread count.
     */
    static native int unreadCount(long shmemPtr);

    /**
     * @param shmemPtr Shared memory pointer.
     * @return Shared memory ID.
     */
    static native int sharedMemoryId(long shmemPtr);

    /**
     * @param shmemPtr Shared memory pointer.
     * @return Semaphore set ID.
     */
    static native int semaphoreId(long shmemPtr);

    /**
     * @param shMemPtr Shared memory pointer
     * @param dest Destination buffer.
     * @param dOff Destination offset.
     * @param size Size.
     * @param timeout Operation timeout.
     * @return Read bytes count.
     * @throws IgniteCheckedException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    static native long readSharedMemoryByteBuffer(long shMemPtr, ByteBuffer dest, long dOff, long size, long timeout)
        throws IgniteCheckedException, GridIpcSharedMemoryOperationTimedoutException;

    /**
     * @param shMemPtr Shared memory pointer
     * @param src Source buffer.
     * @param sOff Offset.
     * @param size Size.
     * @param timeout Operation timeout.
     * @throws IgniteCheckedException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    static native void writeSharedMemory(long shMemPtr, byte src[], long sOff, long size, long timeout)
        throws IgniteCheckedException, GridIpcSharedMemoryOperationTimedoutException;

    /**
     * @param shMemPtr Shared memory pointer
     * @param src Source buffer.
     * @param sOff Offset.
     * @param size Size.
     * @param timeout Operation timeout.
     * @throws IgniteCheckedException If space has been closed.
     * @throws GridIpcSharedMemoryOperationTimedoutException If operation times out.
     */
    static native void writeSharedMemoryByteBuffer(long shMemPtr, ByteBuffer src, long sOff, long size, long timeout)
        throws IgniteCheckedException, GridIpcSharedMemoryOperationTimedoutException;

    /** @return PID of the current process (-1 on error). */
    public static int pid() {
        // Should be something like this: 1160@mbp.local
        String name = ManagementFactory.getRuntimeMXBean().getName();

        try {
            int idx = name.indexOf('@');

            return idx > 0 ? Integer.parseInt(name.substring(0, idx)) : -1;
        }
        catch (NumberFormatException ignored) {
            return -1;
        }
    }

    /**
     * @param pid PID to check.
     * @return {@code True} if process with passed ID is alive.
     */
    static native boolean alive(int pid);

    /**
     * Returns shared memory ids for Mac OS and Linux platforms.
     *
     * @return Collection of all shared memory IDs in the system.
     * @throws IOException If failed.
     * @throws InterruptedException If failed.
     * @throws IllegalStateException If current OS is not supported.
     */
    static Collection<Integer> sharedMemoryIds() throws IOException, InterruptedException {
        if (U.isMacOs() || U.isLinux())
            return sharedMemoryIdsOnMacOS();
        else
            throw new IllegalStateException("Current OS is not supported.");
    }

    /**
     * @param e Link error.
     * @return Wrapping grid exception.
     */
    static IgniteCheckedException linkError(UnsatisfiedLinkError e) {
        return new IgniteCheckedException("Linkage error due to possible native library, libggshmem.so, " +
            "version mismatch (stop all grid nodes, clean up your '/tmp' folder, and try again).", e);
    }

    /**
     * @return Shared memory IDs.
     * @throws IOException If failed.
     * @throws InterruptedException If failed.
     */
    private static Collection<Integer> sharedMemoryIdsOnMacOS() throws IOException, InterruptedException {
        // IPC status from <running system> as of Mon Jan 21 15:33:54 MSK 2013
        // T     ID     KEY        MODE       OWNER    GROUP
        // Shared Memory:
        // m 327680 0x4702fd26 --rw-rw-rw- yzhdanov    staff

        Process proc = Runtime.getRuntime().exec("ipcs -m");

        BufferedReader rdr = new BufferedReader(new InputStreamReader(proc.getInputStream()));

        Collection<Integer> ret = new ArrayList<>();

        try {
            String line;

            while ((line = rdr.readLine()) != null) {
                if (!line.startsWith(getPlatformDependentLineStartFlag()))
                    continue;

                String[] toks = line.split(" ");

                try {
                    ret.add(Integer.parseInt(toks[1]));
                }
                catch (NumberFormatException ignored) {
                    // No-op (just ignore).
                }
            }

            return ret;
        }
        finally {
            proc.waitFor();
        }
    }

    /** @return Flag for {@code ipcs} utility. */
    private static String getPlatformDependentLineStartFlag() {
        if (U.isMacOs())
            return "m ";
        else if (U.isLinux())
            return "0x";
        else
            throw new IllegalStateException("This OS is not supported.");
    }
}
