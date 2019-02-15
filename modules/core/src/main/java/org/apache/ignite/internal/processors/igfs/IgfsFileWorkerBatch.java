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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Work batch is an abstraction of the logically grouped tasks.
 */
public abstract class IgfsFileWorkerBatch implements Runnable {
    /** Stop marker. */
    private static final byte[] FINISH_MARKER = new byte[0];

    /** Cancel marker. */
    private static final byte[] CANCEL_MARKER = new byte[0];

    /** Tasks queue. */
    private final BlockingDeque<byte[]> queue = new LinkedBlockingDeque<>();

    /** Future which completes when batch processing is finished. */
    private final GridFutureAdapter fut = new GridFutureAdapter();

    /** Path to the file in the primary file system. */
    private final IgfsPath path;

    /** Output stream to the file. */
    private final OutputStream out;

    /** Finishing flag. */
    private volatile boolean finishing;

    /**
     * Constructor.
     *
     * @param path Path to the file in the primary file system.
     * @param out Output stream opened to that file.
     */
    IgfsFileWorkerBatch(IgfsPath path, OutputStream out) {
        assert path != null;
        assert out != null;

        this.path = path;
        this.out = out;
    }

    /**
     * Perform write if batch is not finishing yet.
     *
     * @param data Data to be written.
     * @return {@code True} in case write was enqueued.
     */
    synchronized boolean write(final byte[] data) {
        return offer(data, false, false);
    }

    /**
     * Add the last task to that batch which will release all the resources.
     *
     * @return {@code True} if finish was signalled.
     */
    synchronized boolean finish() {
        return offer(FINISH_MARKER, false, true);
    }

    /**
     * Cancel batch processing.
     *
     * @return {@code True} if cancel was signalled.
     */
    synchronized boolean cancel() {
        return offer(CANCEL_MARKER, true, true);
    }

    /**
     * Add request to queue.
     *
     * @param data Data.
     * @param head Whether to add to head.
     * @param finish Whether this is the last batch to be accepted.
     * @return {@code True} if request was added to queue.
     */
    private synchronized boolean offer(byte[] data, boolean head, boolean finish) {
        if (finishing)
            return false;

        if (head)
            queue.addFirst(data);
        else
            queue.addLast(data);

        if (finish)
            finishing = true;

        return true;
    }

    /**
     * @return {@code True} if finish was called on this batch.
     */
    boolean finishing() {
        return finishing;
    }

    /**
     * Process the batch.
     */
    @Override @SuppressWarnings("unchecked")
    public void run() {
        Throwable err = null;

        try {
            while (true) {
                try {
                    byte[] data = queue.poll(1000, TimeUnit.MILLISECONDS);

                    if (data == FINISH_MARKER) {
                        assert queue.isEmpty();

                        break;
                    }
                    else if (data == CANCEL_MARKER)
                        throw new IgfsFileWorkerBatchCancelledException(path);
                    else if (data != null) {
                        try {
                            out.write(data);
                        }
                        catch (IOException e) {
                            throw new IgniteCheckedException("Failed to write data to the file due to secondary " +
                                "file system exception: " + path, e);
                        }
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    err = e;

                    break;
                }
                catch (Exception e) {
                    err = e;

                    break;
                }
            }
        }
        catch (Throwable e) {
            // Safety. This should never happen under normal conditions.
            err = e;

            if (e instanceof Error)
                throw e;
        }
        finally {
            // Order of events is very important here. First, we close the stream so that metadata locks are released.
            // This action must be the very first because otherwise a writer thread could interfere with itself.
            U.closeQuiet(out);

            // Next, we invoke callback so that IgfsImpl is able to enqueue new requests. This is safe because
            // at this point file processing is completed.
            onDone();

            // Finally, we complete the future, so that waiting threads could resume.
            assert !fut.isDone();

            fut.onDone(null, err);
        }
    }

    /**
     * Await for that worker batch to complete.
     *
     * @throws IgniteCheckedException In case any exception has occurred during batch tasks processing.
     */
    void await() throws IgniteCheckedException {
        fut.get();
    }

    /**
     * Get primary file system path.
     *
     * @return Primary file system path.
     */
    IgfsPath path() {
        return path;
    }

    /**
     * Callback invoked when execution finishes.
     */
    protected abstract void onDone();
}