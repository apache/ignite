package org.apache.ignite.internal.processors.datastructures;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/** */
public class ReentrantLockFailover implements Service {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @IgniteInstanceResource
    private Ignite g;

    /** */
    private final GridCacheInternalKey key;

    /** */
    private final String lockName;

    /** */
    private final boolean fair;

    /** */
    private transient IgnitePredicate<Event> listener;

    /** */
    private transient boolean cancel = false;

    /** */
    public ReentrantLockFailover(GridCacheInternalKey key, String lockName, boolean fair) {
        this.key = key;
        this.lockName = lockName;
        this.fair = fair;
    }

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        g.events().stopLocalListen(listener);

        cancel = true;
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) throws Exception {
        listener = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                GridCacheLockEx2Default lock = (GridCacheLockEx2Default)g.reentrantLock(lockName, fair, false);

                if (lock != null)
                    lock.removeAll(event.node().id());

                return true;
            }
        };

        g.events().localListen(listener, new int[]{EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_CLIENT_NODE_DISCONNECTED,
            EVT_CLIENT_NODE_RECONNECTED});
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
    }
}
