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

import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Striped executor.
 */
public class StripedExecutor {
    /** Count. */
    private final int cnt;

    /** Stripes. */
    private final Stripe[] stripes;

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
            Stripe stripe = new Stripe();

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

    public void dumpStats(IgniteLogger log) {
        StringBuilder sb = new StringBuilder("Stats ");

        for (int i = 0; i < stripes.length; i++) {
            sb.append(i)
                .append(" [cnt=").append(stripes[i].cnt)
                .append(", qSize=").append(stripes[i].queue.size())
                .append("]; ");

            stripes[i].cnt = 0;
        }

        if (log.isInfoEnabled())
            log.info(sb.toString());
    }

    /**
     * Stripe.
     */
    private static class Stripe implements Runnable {
        /** Queue. */
//        private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        private final SingleConsumerSpinCircularBuffer<Runnable> queue = new SingleConsumerSpinCircularBuffer<>(256);

        /** Stopping flag. */
        private volatile boolean stopping;

        /** Thread executing the loop. */
        private Thread thread;

        private volatile long cnt;

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
                    cmd = queue.take();
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
        void execute(Runnable cmd) {
//            queue.add(cmd);
            queue.put(cmd);
        }
    }
}
