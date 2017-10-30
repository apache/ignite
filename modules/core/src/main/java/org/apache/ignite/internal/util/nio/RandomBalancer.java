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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.lang.IgniteRunnable;

/**
 * For tests only.
 */
@SuppressWarnings("unchecked")
class RandomBalancer<T> implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    @GridToStringExclude
    private final GridNioServer<T> nio;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /**
     * @param nio Nio server.
     * @param log Logger.
     */
    RandomBalancer(GridNioServer<T> nio, IgniteLogger log) {
        this.nio = nio;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        int w1 = rnd.nextInt(nio.workers().size());

        if (nio.workers().get(w1).sessions().isEmpty())
            return;

        int w2 = rnd.nextInt(nio.workers().size());

        while (w2 == w1)
            w2 = rnd.nextInt(nio.workers().size());

        GridNioSession ses = randomSession(nio.workers().get(w1));

        if (ses != null) {
            if (log.isInfoEnabled())
                log.info("Move session [from=" + w1 +
                    ", to=" + w2 +
                    ", ses=" + ses + ']');

            nio.moveSession(ses, w1, w2);
        }
    }

    /**
     * @param worker Worker.
     * @return NIO session.
     */
    private GridNioSession randomSession(GridNioWorker worker) {
        Collection<GridNioSession> sessions = worker.sessions();

        int size = sessions.size();

        if (size == 0)
            return null;

        int idx = ThreadLocalRandom.current().nextInt(size);

        Iterator<GridNioSession> it = sessions.iterator();

        int cnt = 0;

        while (it.hasNext()) {
            GridNioSession ses = it.next();

            if (cnt == idx)
                return ses;
        }

        return null;
    }

}
