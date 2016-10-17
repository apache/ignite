/*
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */

/*
 *
 *
 *
 *
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.apache.ignite.internal.processors.pool.striped;

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Striped executor.
 */
public class StripedExecutor {
    /** Count. */
    private final int cnt;

    /** Stripes. */
    private final Stripe[] stripes;

    /**
     * Constructor.
     *
     * @param cnt Count.
     */
    public StripedExecutor(int cnt) {
        this.cnt = cnt;

        stripes = new Stripe[cnt];

        for (int i = 0; i < cnt; i++) {
            Stripe stripe = new Stripe();

            stripe.start(i);

            stripes[i] = stripe;
        }
    }

    /**
     * Execute command.
     *
     * @param idx Index.
     * @param cmd Command.
     */
    public void execute(int idx, Runnable cmd) {
        stripes[idx % cnt].execute(cmd);
    }

    /**
     * Stop executor.
     */
    public void stop() {
        for (Stripe stripe : stripes)
            stripe.stop();
    }

    /**
     * Stripe.
     */
    private static class Stripe implements Runnable {
        /** Queue. */
        private final ConcurrentLinkedDeque<Runnable> queue = new ConcurrentLinkedDeque<>();

        /** Lock. */
        private final ReentrantLock lock = new ReentrantLock();

        /** Condition. */
        private final Condition cond = lock.newCondition();

        /** Active flag. */
        private final AtomicBoolean active = new AtomicBoolean(true);

        /** Stopping flag. */
        private volatile boolean stopping;

        /** Thread executing the loop. */
        private Thread thread;

        /**
         * Start the stripe.
         */
        void start(int idx) {
            thread = new Thread(this);

            thread.setName("stripe-" + idx);
            thread.setDaemon(true);

            thread.start();
        }

        /**
         * Stop the stripe.
         */
        void stop() {
            lock.lock();

            try {
                stopping = true;

                cond.signalAll();
            }
            finally {
                lock.unlock();
            }

            try {
                thread.interrupt();

                thread.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stopping) {
                Runnable cmd = queue.pollFirst();

                if (cmd == null) {
                    active.set(false);

                    // Re-check the queue.
                    cmd = queue.pollFirst();

                    if (cmd != null)
                        active.set(true);
                    else {
                        // Failed to get anything from the queue, resort to blocking.
                        try {
                            lock.lock();

                            try {
                                while (!stopping) {
                                    cmd = queue.pollFirst();

                                    if (cmd == null)
                                        cond.await();
                                    else {
                                        active.set(true);

                                        break;
                                    }
                                }
                            }
                            finally {
                                lock.unlock();
                            }
                        }
                        catch (InterruptedException e) {
                            stopping = true;

                            Thread.currentThread().interrupt();
                        }
                    }
                }

                if (cmd != null)
                    execute0(cmd);
            }
        }

        /**
         * Internal execution routine.
         *
         * @param cmd Command.
         */
        private void execute0(Runnable cmd) {
            try {
                cmd.run();
            }
            catch (Exception e) {
                U.warn(null, "Unexpected exception in stripe loop.", e);
            }
        }

        /**
         * Execute the command.
         *
         * @param cmd Command.
         */
        void execute(Runnable cmd) {
            queue.addLast(cmd);

            if (!active.get()) {
                lock.lock();

                try {
                    cond.signalAll();
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }
}
