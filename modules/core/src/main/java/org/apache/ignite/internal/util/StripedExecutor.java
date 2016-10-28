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

package org.apache.ignite.internal.util;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Striped executor.
 */
public class StripedExecutor {
    /** Count. */
    private final int cnt;

    /** Stripes. */
    private final Stripe[] stripes;

    /** */
    private volatile boolean inited;

    /**
     * Constructor.
     *
     * @param cnt Count.
     */
    public StripedExecutor(int cnt) {
        this.cnt = cnt;

        stripes = new Stripe[cnt];

        for (int i = 0; i < cnt; i++) {
            Stripe stripe = new StripeConcurrentQueue();

            stripes[i] = stripe;

            stripe.start(i);
        }

        inited = true;
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
        for (; !inited; )
            ;

        for (Stripe stripe : stripes)
            stripe.signalStop();

        for (Stripe stripe : stripes)
            stripe.awaitStop();
    }

    /**
     * @param log Logger to dump to.
     */
    public void dumpStats(IgniteLogger log) {
        StringBuilder sb = new StringBuilder("Stats ");

        for (int i = 0; i < stripes.length; i++) {
            sb.append(i)
                .append(" [cnt=").append(stripes[i].cnt)
                .append(", qSize=").append(stripes[i].queueSize())
                .append("]; ");

            stripes[i].cnt = 0;
        }

        if (log.isInfoEnabled())
            log.info(sb.toString());
    }

    /**
     * Stripe.
     */
    private static abstract class Stripe implements Runnable {
        /** Stopping flag. */
        private volatile boolean stopping;

        /** Thread executing the loop. */
        protected Thread thread;

        /** */
        private volatile long cnt;

        /**
         * Start the stripe.
         */
        void start(int idx) {
            thread = new Thread(this);

            thread.setName("stripe-" + idx);

            thread.start();
        }

        /**
         * Stop the stripe.
         */
        void signalStop() {
            stopping = true;

            thread.interrupt();
        }

        /**
         * Await thread stop.
         */
        void awaitStop() {
            try {
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
                Runnable cmd;

                try {
                    cmd = take();
                }
                catch (InterruptedException e) {
                    stopping = true;

                    Thread.currentThread().interrupt();

                    return;
                }

                if (cmd != null)
                    execute0(cmd);

                cnt++;
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
        abstract void execute(Runnable cmd);

        /**
         * @return Next runnable.
         * @throws InterruptedException If interrupted.
         */
        abstract Runnable take() throws InterruptedException;

        /**
         * @return Queue size.
         */
        abstract int queueSize();

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Stripe.class, this);
        }
    }

    /**
     * Stripe.
     */
    private static class StripeSpinCircularBuffer extends Stripe {
        /** Queue. */
        private final SingleConsumerSpinCircularBuffer<Runnable> queue = new SingleConsumerSpinCircularBuffer<>(256);

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            return queue.take();
        }

        /** {@inheritDoc} */
        void execute(Runnable cmd) {
            queue.add(cmd);
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeSpinCircularBuffer.class, this, super.toString());
        }
    }

    /**
     * Stripe.
     */
    private static class StripeConcurrentQueue extends Stripe {
        /** Queue. */
        private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        /** */
        private volatile boolean parked;

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            Runnable r = queue.poll();

            if (r != null)
                return r;

            parked = true;

            try {
                for (;;) {
                    r = queue.poll();

                    if (r != null)
                        return r;

                    LockSupport.park();

                    if (Thread.interrupted())
                        throw new InterruptedException();
                }
            }
            finally {
                parked = false;
            }
        }

        /** {@inheritDoc} */
        void execute(Runnable cmd) {
            queue.add(cmd);

            if (parked)
                LockSupport.unpark(thread);
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeConcurrentQueue.class, this, super.toString());
        }
    }

    /**
     * Stripe.
     */
    private static class StripeConcurrentBlockingQueue extends Stripe {
        /** Queue. */
        private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            return queue.take();
        }

        /** {@inheritDoc} */
        void execute(Runnable cmd) {
            queue.add(cmd);
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeConcurrentBlockingQueue.class, this, super.toString());
        }
    }
}
