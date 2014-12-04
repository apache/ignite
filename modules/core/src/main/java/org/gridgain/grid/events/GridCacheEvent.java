/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * In-memory database (cache) event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link GridEvents#remoteQuery(GridPredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link GridEvents#localQuery(GridPredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link GridEvents#localListen(GridPredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link GridEvents#waitForLocal(GridPredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link GridConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 * @see GridEventType#EVT_CACHE_ENTRY_CREATED
 * @see GridEventType#EVT_CACHE_ENTRY_DESTROYED
 * @see GridEventType#EVT_CACHE_ENTRY_EVICTED
 * @see GridEventType#EVT_CACHE_OBJECT_PUT
 * @see GridEventType#EVT_CACHE_OBJECT_READ
 * @see GridEventType#EVT_CACHE_OBJECT_REMOVED
 * @see GridEventType#EVT_CACHE_OBJECT_LOCKED
 * @see GridEventType#EVT_CACHE_OBJECT_UNLOCKED
 * @see GridEventType#EVT_CACHE_OBJECT_SWAPPED
 * @see GridEventType#EVT_CACHE_OBJECT_UNSWAPPED
 * @see GridEventType#EVT_CACHE_OBJECT_EXPIRED
 */
public class GridCacheEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String cacheName;

    /** Partition for the event. */
    private int part;

    /** Cache entry. */
    @GridToStringInclude
    private Object key;

    /** Event ID. */
    @GridToStringInclude
    private final GridUuid xid;

    /** Lock ID. */
    @GridToStringInclude
    private final Object lockId;

    /** New value. */
    @GridToStringInclude
    private final Object newVal;

    /** Old value. */
    @GridToStringInclude
    private final Object oldVal;

    /**
     * Flag indicating whether old value is present in case if we
     * don't have it in deserialized form.
     */
    @GridToStringInclude
    private final boolean hasOldVal;

    /**
     * Flag indicating whether new value is present in case if we
     * don't have it in deserialized form.
     */
    @GridToStringInclude
    private final boolean hasNewVal;

    /** Event node. */
    @GridToStringExclude
    @Nullable private final ClusterNode evtNode;

    /** Flag indicating whether event happened on {@code near} or {@code partitioned} cache. */
    @GridToStringInclude
    private boolean near;

    /** Subject ID. */
    @GridToStringInclude
    private UUID subjId;

    /** Closure class name. */
    @GridToStringInclude
    private String cloClsName;

    /** Task name if update was initiated within task execution. */
    @GridToStringInclude
    private String taskName;

    /**
     * Constructs cache event.
     *
     * @param cacheName Cache name.
     * @param node Local node.
     * @param evtNode Event node ID.
     * @param msg Event message.
     * @param type Event type.
     * @param part Partition for the event (usually the partition the key belongs to).
     * @param near Flag indicating whether event happened on {@code near} or {@code partitioned} cache.
     * @param key Cache key.
     * @param xid Transaction ID.
     * @param lockId Lock ID.
     * @param newVal New value.
     * @param hasNewVal Flag indicating whether new value is present in case if we
     *      don't have it in deserialized form.
     * @param oldVal Old value.
     * @param hasOldVal Flag indicating whether old value is present in case if we
     *      don't have it in deserialized form.
     * @param subjId Subject ID.
     * @param cloClsName Closure class name.
     */
    public GridCacheEvent(String cacheName, ClusterNode node, @Nullable ClusterNode evtNode, String msg, int type, int part,
        boolean near, Object key, GridUuid xid, Object lockId, Object newVal, boolean hasNewVal,
        Object oldVal, boolean hasOldVal, UUID subjId, String cloClsName, String taskName) {
        super(node, msg, type);
        this.cacheName = cacheName;
        this.evtNode = evtNode;
        this.part = part;
        this.near = near;
        this.key = key;
        this.xid = xid;
        this.lockId = lockId;
        this.newVal = newVal;
        this.hasNewVal = hasNewVal;
        this.oldVal = oldVal;
        this.hasOldVal = hasOldVal;
        this.subjId = subjId;
        this.cloClsName = cloClsName;
        this.taskName = taskName;
    }

    /**
     * Gets cache name.
     *
     * @return Cache name.
     */
    @Nullable public String cacheName() {
        return cacheName;
    }

    /**
     * Gets partition for the event which is the partition the key belongs to.
     *
     * @return Partition for the event.
     */
    public int partition() {
        return part;
    }

    /**
     * Gets flag indicating whether event happened on {@code near} or {@code partitioned} cache.
     *
     * @return Flag indicating whether event happened on {@code near} or {@code partitioned} cache.
     */
    public boolean isNear() {
        return near;
    }

    /**
     * Gets node which initiated cache operation or {@code null} if that node is not available.
     *
     * @return Node which initiated cache operation or {@code null} if that node is not available.
     */
    @Nullable public ClusterNode eventNode() {
        return evtNode;
    }

    /**
     * Gets cache entry associated with event.
     *
     * @return Cache entry associated with event.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <K> K key() {
        return (K)key;
    }

    /**
     * ID of surrounding cache cache transaction or <tt>null</tt> if there is
     * no surrounding transaction.
     *
     * @return ID of surrounding cache transaction.
     */
    @Nullable public GridUuid xid() {
        return xid;
    }

    /**
     * ID of the lock if held or <tt>null</tt> if no lock held.
     *
     * @return ID of the lock if held.
     */
    @Nullable public Object lockId() {
        return lockId;
    }

    /**
     * Gets new value for this event.
     *
     * @return New value associated with event (<tt>null</tt> if event is
     *      {@link GridEventType#EVT_CACHE_OBJECT_REMOVED}.
     */
    @Nullable public Object newValue() {
        return newVal;
    }

    /**
     * Gets old value associated with this event.
     *
     * @return Old value associated with event.
     */
    @Nullable public Object oldValue() {
        return oldVal;
    }

    /**
     * Gets flag indicating whether cache entry has old value in case if
     * we only have old value in serialized form in which case {@link #oldValue()}
     * will return {@code null}.
     *
     * @return Flag indicating whether there is old value associated with this event.
     */
    public boolean hasOldValue() {
        return hasOldVal;
    }

    /**
     * Gets flag indicating whether cache entry has new value in case if
     * we only have new value in serialized form in which case {@link #newValue()}
     * will return {@code null}.
     *
     * @return Flag indicating whether there is new value associated with this event.
     */
    public boolean hasNewValue() {
        return hasNewVal;
    }

    /**
     * Gets security subject ID initiated this cache event, if available. This property is available only for
     * {@link GridEventType#EVT_CACHE_OBJECT_PUT}, {@link GridEventType#EVT_CACHE_OBJECT_REMOVED} and
     * {@link GridEventType#EVT_CACHE_OBJECT_READ} cache events.
     * <p>
     * Subject ID will be set either to nodeId initiated cache update or read or client ID initiated
     * cache update or read.
     *
     * @return Subject ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
    }

    /**
     * Gets closure class name (applicable only for TRANSFORM operations).
     *
     * @return Closure class name.
     */
    @Nullable public String closureClassName() {
        return cloClsName;
    }

    /**
     * Gets task name if cache event was caused by an operation initiated within task execution.
     *
     * @return Task name.
     */
    @Nullable public String taskName() {
        return taskName;
    }

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return name() + ": near=" + near + ", key=" + key + ", hasNewVal=" + hasNewVal + ", hasOldVal=" + hasOldVal +
            ", nodeId8=" + U.id8(node().id());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public String toString() {
        return S.toString(GridCacheEvent.class, this,
            "nodeId8", U.id8(node().id()),
            "evtNodeId8", U.id8(evtNode.id()),
            "msg", message(),
            "type", name(),
            "tstamp", timestamp());
    }
}
