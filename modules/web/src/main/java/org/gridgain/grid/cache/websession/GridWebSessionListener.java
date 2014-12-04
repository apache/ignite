/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.websession;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Session listener for web sessions caching.
 */
class GridWebSessionListener {
    /** */
    private static final long RETRY_DELAY = 1;

    /** Cache. */
    private final GridCache<String, GridWebSession> cache;

    /** Maximum retries. */
    private final int retries;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ignite Grid.
     * @param cache Cache.
     * @param retries Maximum retries.
     */
    GridWebSessionListener(Ignite ignite, GridCache<String, GridWebSession> cache, int retries) {
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
            if (cache.removex(sesId) && log.isDebugEnabled())
                log.debug("Session destroyed: " + sesId);
        }
        catch (GridException e) {
            U.error(log, "Failed to remove session: " + sesId, e);
        }
    }

    /**
     * @param sesId Session ID.
     * @param updates Updates list.
     * @param maxInactiveInterval Max session inactive interval.
     */
    public void updateAttributes(String sesId, Collection<T2<String, Object>> updates, int maxInactiveInterval) {
        assert sesId != null;
        assert updates != null;

        if (log.isDebugEnabled())
            log.debug("Session attributes updated [id=" + sesId + ", updates=" + updates + ']');

        try {
            for (int i = 0; i < retries; i++) {
                try {
                    GridCacheEntry<String, GridWebSession> entry = cache.entry(sesId);

                    assert entry != null;

                    if (maxInactiveInterval < 0)
                        maxInactiveInterval = 0;

                    entry.timeToLive(maxInactiveInterval * 1000);

                    entry.transform(new AttributesUpdated(updates));

                    break;
                }
                catch (GridCachePartialUpdateException ignored) {
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
        catch (GridException e) {
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
    private static class AttributesUpdated implements C1<GridWebSession, GridWebSession>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Updates list. */
        private Collection<T2<String, Object>> updates;

        /**
         * Required by {@link Externalizable}.
         */
        public AttributesUpdated() {
            // No-op.
        }

        /**
         * @param updates Updates list.
         */
        AttributesUpdated(Collection<T2<String, Object>> updates) {
            assert updates != null;

            this.updates = updates;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NonSerializableObjectBoundToHttpSession")
        @Nullable @Override public GridWebSession apply(@Nullable GridWebSession ses) {
            if (ses == null)
                return null;

            ses = new GridWebSession(ses);

            for (T2<String, Object> update : updates) {
                String name = update.get1();

                assert name != null;

                Object val = update.get2();

                if (val != null)
                    ses.setAttribute(name, val);
                else
                    ses.removeAttribute(name);
            }

            return ses;
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
