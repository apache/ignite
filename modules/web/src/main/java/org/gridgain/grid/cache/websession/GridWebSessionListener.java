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

package org.gridgain.grid.cache.websession;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Session listener for web sessions caching.
 */
class GridWebSessionListener {
    /** */
    private static final long RETRY_DELAY = 1;

    /** Cache. */
    private final IgniteCache<String, GridWebSession> cache;

    /** Maximum retries. */
    private final int retries;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ignite Grid.
     * @param cache Cache.
     * @param retries Maximum retries.
     */
    GridWebSessionListener(Ignite ignite, IgniteCache<String, GridWebSession> cache, int retries) {
        assert ignite != null;
        assert cache != null;

        this.cache = cache;
        this.retries = retries > 0 ? retries : 1;

        log = ignite.log();
    }

    /**
     * @param sesId Session ID.
     */
    public void destroySession(String sesId) {
        assert sesId != null;

        try {
            if (cache.remove(sesId) && log.isDebugEnabled())
                log.debug("Session destroyed: " + sesId);
        }
        catch (CacheException e) {
            U.error(log, "Failed to remove session: " + sesId, e);
        }
    }

    /**
     * @param sesId Session ID.
     * @param updates Updates list.
     * @param maxInactiveInterval Max session inactive interval.
     */
    @SuppressWarnings("unchecked")
    public void updateAttributes(String sesId, Collection<T2<String, Object>> updates, int maxInactiveInterval) {
        assert sesId != null;
        assert updates != null;

        if (log.isDebugEnabled())
            log.debug("Session attributes updated [id=" + sesId + ", updates=" + updates + ']');

        try {
            for (int i = 0; i < retries; i++) {
                try {
                    IgniteCache<String, GridWebSession> cache0;

                    if (maxInactiveInterval > 0) {
                        long ttl = maxInactiveInterval * 1000;

                        ExpiryPolicy plc = new ModifiedExpiryPolicy(new Duration(MILLISECONDS, ttl));

                        cache0 = cache.withExpiryPolicy(plc);
                    }
                    else
                        cache0 = cache;

                    cache0.invoke(sesId, new AttributesProcessor(updates));

                    break;
                }
                catch (CachePartialUpdateException ignored) {
                    if (i == retries - 1) {
                        U.warn(log, "Failed to apply updates for session (maximum number of retries exceeded) [sesId=" +
                            sesId + ", retries=" + retries + ']');
                    }
                    else {
                        U.warn(log, "Failed to apply updates for session (will retry): " + sesId);

                        U.sleep(RETRY_DELAY);
                    }
                }
            }
        }
        catch (CacheException | IgniteCheckedException e) {
            U.error(log, "Failed to update session attributes [id=" + sesId + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridWebSessionListener.class, this);
    }

    /**
     * Multiple attributes update transformer.
     */
    private static class AttributesProcessor implements EntryProcessor<String, GridWebSession, Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Updates list. */
        private Collection<T2<String, Object>> updates;

        /**
         * Required by {@link Externalizable}.
         */
        public AttributesProcessor() {
            // No-op.
        }

        /**
         * @param updates Updates list.
         */
        AttributesProcessor(Collection<T2<String, Object>> updates) {
            assert updates != null;

            this.updates = updates;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<String, GridWebSession> entry, Object... args) {
            if (!entry.exists())
                return null;

            GridWebSession ses = new GridWebSession(entry.getValue());

            for (T2<String, Object> update : updates) {
                String name = update.get1();

                assert name != null;

                Object val = update.get2();

                if (val != null)
                    ses.setAttribute(name, val);
                else
                    ses.removeAttribute(name);
            }

            entry.setValue(ses);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeCollection(out, updates);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            updates = U.readCollection(in);
        }
    }
}
