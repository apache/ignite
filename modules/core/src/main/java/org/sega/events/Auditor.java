package org.sega.events;

import java.io.Serializable;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;

/**
 * Class auditor in the constructor, accepts a cache object and an audit object. Creates a continuous request and sets
 * up a local event listener, logs events and sends them to the audit system.
 */
public class Auditor<T, E> implements Serializable {

    @IgniteInstanceResource
    private Ignite ignite;

    private ClusterNode node;

    private Audit audit;

    private Factory factory;

    private CacheEntryEventFilter cacheEntryEventFilter;

//    final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private IgniteCache<T, E> cache;

    private Affinity<T> aff;

    /**
     * @param cache Cache.
     * @param audit Audit.
     */
    Auditor(IgniteCache<T, E> cache, Audit audit) {
        this.cache = cache;
        this.audit = audit;
    }

    /**
     *
     */
    void init() {

        factory = new Factory<CacheEntryEventSerializableFilter<? extends T,? extends E>> (){
            @Override public CacheEntryEventSerializableFilter<T,E> create() {
                return new CacheEntryEventSerializableFilter<T,E>() {
                    @Override public boolean evaluate(CacheEntryEvent<? extends T, ? extends E> evt) throws CacheEntryListenerException {
                        if (nodesPrimaryForThisKey(evt.getKey())) {
                            accept(evt);
                            return true;
                        }
                        return false;
                    }
                };
            }
        };


        ContinuousQuery<T, E> qry = new ContinuousQuery<>();
        qry.setAutoUnsubscribe(false);

//        qry.setRemoteFilterFactory(factory);

        qry.setRemoteFilter(new CacheEntryEventSerializableFilter<T, E>() {
            @Override public boolean evaluate(
                CacheEntryEvent<? extends T, ? extends E> evt) throws CacheEntryListenerException {

                if (nodesPrimaryForThisKey(evt.getKey())) {
                    accept(evt);
                    return true;
                }
                return false;
            }
        });

        qry.setLocalListener(evnts -> evnts.forEach(e -> {
            System.out.println("i'm local "+e.getEventType().name());
//            accept(e);
        }));

        cache.query(qry);
    }

    /**
     * @param event Event.
     */
    protected void accept(@NotNull CacheEntryEvent event) {

        EventType type = event.getEventType();
        E newVal, oldVal;
        newVal = (E)event.getValue();
        String msg;

        switch (type) {
            case CREATED:
                msg = "Created new Entity key=" + event.getKey() + " value={" + newVal.toString() + "}";
                sendMessage(msg);
                break;

            case REMOVED:
                oldVal = (E)event.getOldValue();
                msg = "Remove Entity key=" + event.getKey()
                    + " value={" + oldVal.toString() + "}";
                sendMessage(msg);
                break;

            case UPDATED:
                oldVal = (E)event.getOldValue();
                msg = "Update Entity key=" + event.getKey() + " "
                    + "OLD value={" + oldVal.toString() + "}, "
                    + "NEW value={" + newVal.toString() + "}";
                sendMessage(msg);
                break;

            default:
                msg = "UNKNOWN EVENT=" + event.toString();
                sendMessage(msg);
                break;
        }

    }

    /**
     * @param msg Message.
     */
    void sendMessage(String msg) {
//        LOGGER.info(message);
//        System.out.println(message);
        audit.sendMessage(msg);

    }

    boolean nodesPrimaryForThisKey(T key) {
        aff = ignite.affinity(cache.getName());
        node = ignite.cluster().localNode();
        ClusterNode primary = aff.mapKeyToNode(key);
        return primary.id().equals(node.id());
    }

}