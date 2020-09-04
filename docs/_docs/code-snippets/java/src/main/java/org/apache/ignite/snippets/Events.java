package org.apache.ignite.snippets;

import java.util.Collection;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;

public class Events {

    public static void main(String[] args) {

    }

    void enablingEvents() {
        // tag::enabling-events[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Enable cache events.
        cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_READ,
                EventType.EVT_CACHE_OBJECT_REMOVED, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::enabling-events[]

        // tag::get-events[]
        IgniteEvents events = ignite.events();
        // end::get-events[]
    }

    void getEventsForNodes() {
        // tag::get-events-for-cache[]
        Ignite ignite = Ignition.ignite();

        IgniteEvents events = ignite.events(ignite.cluster().forCacheNodes("person"));
        // end::get-events-for-cache[]
    }

    void getNodeFromEvent(Ignite ignite) {
        // tag::get-node[]
        IgniteEvents events = ignite.events();

        UUID uuid = events.remoteListen(new IgniteBiPredicate<UUID, JobEvent>() {
            @Override
            public boolean apply(UUID uuid, JobEvent e) {

                System.out.println("nodeID = " + e.node().id() + ", addresses=" + e.node().addresses());

                return true; //continue listening
            }
        }, null, EventType.EVT_JOB_FINISHED);

        // end::get-node[]
    }

    void localEvents(Ignite ignite) {
        // tag::local[]
        IgniteEvents events = ignite.events();

        // Local listener that listens to local events.
        IgnitePredicate<CacheEvent> localListener = evt -> {
            System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() + ", oldVal=" + evt.oldValue()
                    + ", newVal=" + evt.newValue());

            return true; // Continue listening.
        };

        // Subscribe to specified cache events occurring on the local node.
        events.localListen(localListener, EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_READ,
                EventType.EVT_CACHE_OBJECT_REMOVED);
        // end::local[]
    }

    void remoteEvents(Ignite ignite) {
        // tag::remote[]
        IgniteEvents events = ignite.events();

        IgnitePredicate<CacheEvent> filter = evt -> {
            System.out.println("remote event: " + evt.name());
            return true;
        };

        // Subscribe to specified cache events on all nodes that have cache running.
        UUID uuid = events.remoteListen(new IgniteBiPredicate<UUID, CacheEvent>() {

            @Override
            public boolean apply(UUID uuid, CacheEvent e) {

                // process the event

                return true; //continue listening
            }
        }, filter, EventType.EVT_CACHE_OBJECT_PUT);
        // end::remote[]
    }

    void batching() {
        // tag::batching[]
        Ignite ignite = Ignition.ignite();

        // Get an instance of named cache.
        final IgniteCache<Integer, String> cache = ignite.cache("cacheName");

        // Sample remote filter which only accepts events for keys
        // that are greater than or equal to 10.
        IgnitePredicate<CacheEvent> rmtLsnr = new IgnitePredicate<CacheEvent>() {
            @Override
            public boolean apply(CacheEvent evt) {
                System.out.println("Cache event: " + evt);

                int key = evt.key();

                return key >= 10;
            }
        };

        // Subscribe to cache events occurring on all nodes
        // that have the specified cache running.
        // Send notifications in batches of 10.
        ignite.events(ignite.cluster().forCacheNodes("cacheName")).remoteListen(10 /* batch size */,
                0 /* time intervals */, false, null, rmtLsnr, EventType.EVTS_CACHE);

        // Generate cache events.
        for (int i = 0; i < 20; i++)
            cache.put(i, Integer.toString(i));

        // end::batching[]
    }

    void storeEvents() {
        // tag::event-storage[]
        MemoryEventStorageSpi eventStorageSpi = new MemoryEventStorageSpi();
        eventStorageSpi.setExpireAgeMs(600000);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setEventStorageSpi(eventStorageSpi);

        Ignite ignite = Ignition.start(igniteCfg);
        // end::event-storage[]

        IgniteEvents events = ignite.events();

        // tag::query-local-events[]
        Collection<CacheEvent> cacheEvents = events.localQuery(e -> {
            // process the event
            return true;
        }, EventType.EVT_CACHE_OBJECT_PUT);

        // end::query-local-events[]

        // tag::query-remote-events[]
        Collection<CacheEvent> storedEvents = events.remoteQuery(e -> {
            // process the event
            return true;
        }, 0, EventType.EVT_CACHE_OBJECT_PUT);

        // end::query-remote-events[]

        ignite.close();
    }

}
