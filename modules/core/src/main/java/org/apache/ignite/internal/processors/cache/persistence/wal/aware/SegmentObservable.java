/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.aware;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Implementation of observer-observable pattern. For handling specific changes of segment.
 */
public abstract class SegmentObservable {
    /** Observers for handle changes of archived index. */
    private final List<Consumer<Long>> observers = new ArrayList<>();

    /**
     * @param observer Observer for notification about segment's changes.
     */
    synchronized void addObserver(Consumer<Long> observer) {
        observers.add(observer);
    }

    /**
     * Notify observers about changes.
     *
     * @param segmentId Segment which was been changed.
     */
    synchronized void notifyObservers(long segmentId) {
        observers.forEach(observer -> observer.accept(segmentId));
    }
}
