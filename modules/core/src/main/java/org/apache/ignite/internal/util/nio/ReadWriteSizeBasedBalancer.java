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

package org.apache.ignite.internal.util.nio;

import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;

/**
 *
 */
class ReadWriteSizeBasedBalancer<T> implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    @GridToStringExclude
    private final GridNioServer<T> nio;

    /** */
    private final long balancePeriod;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** */
    private long lastBalance;

    /**
     * @param nio Nio server.
     * @param balancePeriod Period.
     * @param log Logger
     */
    ReadWriteSizeBasedBalancer(GridNioServer<T> nio, long balancePeriod, IgniteLogger log) {
        this.nio = nio;
        this.balancePeriod = balancePeriod;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        long now = U.currentTimeMillis();

        if (lastBalance + balancePeriod < now) {
            lastBalance = now;

            long maxRcvd0 = -1, minRcvd0 = -1, maxSent0 = -1, minSent0 = -1;
            int maxRcvdIdx = -1, minRcvdIdx = -1, maxSentIdx = -1, minSentIdx = -1;

            List<GridNioWorker> workers = nio.workers();

            for (int i = 0; i < workers.size(); i++) {
                GridNioWorker worker = workers.get(i);

                int sesCnt = worker.sessions().size();

                if (i % 2 == 0) {
                    // Reader.
                    long bytesRcvd0 = worker.bytesReceivedSinceRebalancing();

                    if ((maxRcvd0 == -1 || bytesRcvd0 > maxRcvd0) && bytesRcvd0 > 0 && sesCnt > 1) {
                        maxRcvd0 = bytesRcvd0;
                        maxRcvdIdx = i;
                    }

                    if (minRcvd0 == -1 || bytesRcvd0 < minRcvd0) {
                        minRcvd0 = bytesRcvd0;
                        minRcvdIdx = i;
                    }
                }
                else {
                    // Writer.
                    long bytesSent0 = worker.bytesSentSinceRebalancing();

                    if ((maxSent0 == -1 || bytesSent0 > maxSent0) && bytesSent0 > 0 && sesCnt > 1) {
                        maxSent0 = bytesSent0;
                        maxSentIdx = i;
                    }

                    if (minSent0 == -1 || bytesSent0 < minSent0) {
                        minSent0 = bytesSent0;
                        minSentIdx = i;
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Balancing data [minSent0=" + minSent0 + ", minSentIdx=" + minSentIdx +
                    ", maxSent0=" + maxSent0 + ", maxSentIdx=" + maxSentIdx +
                    ", minRcvd0=" + minRcvd0 + ", minRcvdIdx=" + minRcvdIdx +
                    ", maxRcvd0=" + maxRcvd0 + ", maxRcvdIdx=" + maxRcvdIdx + ']');

            if (maxSent0 != -1 && minSent0 != -1) {
                GridNioSession ses = null;

                long sentDiff = maxSent0 - minSent0;
                long delta = sentDiff;
                double threshold = sentDiff * 0.9;

                Set<GridNioSession> sessions =
                    workers.get(maxSentIdx).sessions();

                for (GridNioSession ses0 : sessions) {
                    long bytesSent0 = ses0.bytesSentSinceRebalancing();

                    if (bytesSent0 < threshold &&
                        (ses == null || delta > U.safeAbs(bytesSent0 - sentDiff / 2))) {
                        ses = ses0;
                        delta = U.safeAbs(bytesSent0 - sentDiff / 2);
                    }
                }

                if (ses != null) {
                    if (log.isDebugEnabled())
                        log.debug("Will move session to less loaded writer [ses=" + ses +
                            ", from=" + maxSentIdx + ", to=" + minSentIdx + ']');

                    nio.moveSession(ses, maxSentIdx, minSentIdx);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Unable to find session to move for writers.");
                }
            }

            if (maxRcvd0 != -1 && minRcvd0 != -1) {
                GridNioSession ses = null;

                long rcvdDiff = maxRcvd0 - minRcvd0;
                long delta = rcvdDiff;
                double threshold = rcvdDiff * 0.9;

                Set<GridNioSession> sessions =
                    workers.get(maxRcvdIdx).sessions();

                for (GridNioSession ses0 : sessions) {
                    long bytesRcvd0 = ses0.bytesReceivedSinceRebalancing();

                    if (bytesRcvd0 < threshold &&
                        (ses == null || delta > U.safeAbs(bytesRcvd0 - rcvdDiff / 2))) {
                        ses = ses0;
                        delta = U.safeAbs(bytesRcvd0 - rcvdDiff / 2);
                    }
                }

                if (ses != null) {
                    if (log.isDebugEnabled())
                        log.debug("Will move session to less loaded reader [ses=" + ses +
                            ", from=" + maxRcvdIdx + ", to=" + minRcvdIdx + ']');

                    nio.moveSession(ses, maxRcvdIdx, minRcvdIdx);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Unable to find session to move for readers.");
                }
            }

            for (int i = 0; i < workers.size(); i++) {
                GridNioWorker worker = workers.get(i);

                worker.reset();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ReadWriteSizeBasedBalancer.class, this, super.toString());
    }
}
