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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;

/**
 *
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
class SizeBasedBalancer<T> implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    @GridToStringExclude
    private GridNioServer<T> nio;

    /** */
    private final long balancePeriod;

    @GridToStringExclude
    private final IgniteLogger log;

    /** */
    private long lastBalance;

    /**
     * @param nio Nio server.
     * @param balancePeriod Period.
     * @param log Logger
     */
    SizeBasedBalancer(GridNioServer<T> nio, long balancePeriod, IgniteLogger log) {
        this.nio = nio;
        this.balancePeriod = balancePeriod;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        long now = U.currentTimeMillis();

        if (lastBalance + balancePeriod < now) {
            lastBalance = now;

            long maxBytes0 = -1, minBytes0 = -1;
            int maxBytesIdx = -1, minBytesIdx = -1;

            List<GridNioWorker> workers = nio.workers();
            for (int i = 0; i < workers.size(); i++) {
                GridNioWorker worker = workers.get(i);

                int sesCnt = worker.sessions().size();

                long bytes0 = worker.bytesReceivedSinceRebalancing() + worker.bytesSentSinceRebalancing();

                if ((maxBytes0 == -1 || bytes0 > maxBytes0) && bytes0 > 0 && sesCnt > 1) {
                    maxBytes0 = bytes0;
                    maxBytesIdx = i;
                }

                if (minBytes0 == -1 || bytes0 < minBytes0) {
                    minBytes0 = bytes0;
                    minBytesIdx = i;
                }
            }

            if (log.isDebugEnabled())
                log.debug("Balancing data [min0=" + minBytes0 + ", minIdx=" + minBytesIdx +
                    ", max0=" + maxBytes0 + ", maxIdx=" + maxBytesIdx + ']');

            if (maxBytes0 != -1 && minBytes0 != -1) {
                GridSelectorNioSessionImpl ses = null;

                long bytesDiff = maxBytes0 - minBytes0;
                long delta = bytesDiff;
                double threshold = bytesDiff * 0.9;

                Set<GridSelectorNioSessionImpl> sessions =
                    workers.get(maxBytesIdx).sessions();

                for (GridSelectorNioSessionImpl ses0 : sessions) {
                    long bytesSent0 = ses0.bytesSentSinceRebalancing();

                    if (bytesSent0 < threshold &&
                        (ses == null || delta > U.safeAbs(bytesSent0 - bytesDiff / 2))) {
                        ses = ses0;
                        delta = U.safeAbs(bytesSent0 - bytesDiff / 2);
                    }
                }

                if (ses != null) {
                    if (log.isDebugEnabled())
                        log.debug("Will move session to less loaded worker [ses=" + ses +
                            ", from=" + maxBytesIdx + ", to=" + minBytesIdx + ']');

                    nio.moveSession(ses, maxBytesIdx, minBytesIdx);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Unable to find session to move.");
                }
            }

            for (int i = 0; i < workers.size(); i++) {
                GridNioWorker worker = workers.get(i);

                worker.reset();
            }
        }
    }
}
