package org.apache.ignite.rs;


import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.reactivestreams.Publisher;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Continuous Query implementation over Reactive Streams semantics
 *
 * Streams are abstraction over data flow (possibly infinite) and can be Hot (keep broadcasting irrespective of any
 * subscriber available) or Cold (start generating event once a subscriber connects, but later added subscribers
 * will receive only new event). Cold is suitable for streaming peer to peer.
 *
 * Streams can be replayed (if recorded)
 *
 * Continuous Query is querying streams. As its querying cache (recorded stream), can be replayed (initial query result),
 * if requested and captured/filtered further stream data can be streamed as query result.
 *
 * As initial query result, query cache with same filter (which will be used to filter stream), concat this
 * with future event stream.
 *
 * Issues in integrating RS for ContinuousQuery:
 * Cache local listener (accepts IgniteBiPredicate) does not provide cache data in event notification, so
 * adapting push API (event handler) as pull APi (Iterable with buffering).
 *
 * @param <K>
 * @param <V>
 */
public class ReactiveContinuousQuery<K, V> extends IgniteReactiveStreams implements Serializable {


    public ReactiveContinuousQuery(IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter){
        super(buildContinuousQueryPublisher(cache, filter));
    }

    private static <K, V>Publisher buildContinuousQueryPublisher(IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter){
        ContinuousQuery continuousQuery = new ContinuousQuery<>(); //create ContinuousQuery

        /** initial query result should be same as filters applied over stream */
        continuousQuery.setInitialQuery(new ScanQuery<>(filter));

        /** Adapter for ContinuousQuery local event listener to RS Publisher */
        LocalListenerIterable listener = new LocalListenerIterable();
        continuousQuery.setLocalListener(listener);

        /** query criteria */
        continuousQuery.setRemoteFilter(new CacheEntryEventSerializableFilter<K, V>() {
            @Override
            public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> e) throws CacheEntryListenerException {
                return filter.apply(e.getKey(), e.getValue());
            }
        });
        /** Register listeners and filters */
        cache.query(continuousQuery);
        return IgniteReactiveStreams.iterablesToPublisher(listener);
    }

    /**
     * Adapter class to adapt event lister as stream Publisher (as Iterable). Instances sof this class will be
     * shared with ContinuousQuery (as Local listener) and RS Publisher (composed as Iterable).
     *
     * Events will be buffered and iterator API will iterate over buffer.
     *
     * Note: This is only to demonstrate wrapper over ContinuousQuery. Adapter push API as pull APi (with buffering)
     * is bad design.
     * @param <K>
     * @param <V>
     */
    static class LocalListenerIterable<K, V> implements Iterable<Cache.Entry<K, V>>, CacheEntryUpdatedListener<K, V> {

        private BlockingQueue<Cache.Entry<K, V>> blockingQueue = new LinkedBlockingDeque<>(100);

        public LocalListenerIterable(){

        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> iterable) throws CacheEntryListenerException {
            for(CacheEntryEvent<? extends K, ? extends V> event : iterable){
                blockingQueue.offer(new CacheEntryDTO<>(event.getKey(), event.getValue()));
            }
        }

        @Override
        public Iterator<Cache.Entry<K, V>> iterator() {
            return new Iterator<Cache.Entry<K, V>>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public Cache.Entry<K, V> next() {
                    for(int i=0; i<3; i++){
                        try {
                            Cache.Entry<K, V> data = blockingQueue.take();
                            return data;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    return null;
                }
            };
        }
    }

}
