package de.kp.works.ignite;


import org.apache.ignite.binary.*;

import java.util.Optional;

import org.apache.ignite.*;

public class IgniteConnect
{
    private final String graphNS;
    private Ignite ignite;
    
    
    public static final class IgniteConnect$
    {
        
        private Optional<IgniteConnect> instance;
        private Optional<String> graphNS;
        
        private Optional<IgniteConnect> instance() {
            return this.instance;
        }
        
        private void instance_$eq(final Optional<IgniteConnect> x$1) {
            this.instance = x$1;
        }
        
        private Optional<String> graphNS() {
            return this.graphNS;
        }
        
        private void graphNS_$eq(final Optional<String> x$1) {
            this.graphNS = x$1;
        }
        
        public IgniteConnect getInstance(final String namespace) {
            this.graphNS_$eq((Optional<String>)Optional.of(namespace));
            if (!this.instance().isPresent()) {
                this.instance_$eq((Optional<IgniteConnect>)Optional.of(new IgniteConnect(namespace)));
            }
            return (IgniteConnect)this.instance().get();
        }
        
        public String namespace() {
            return this.graphNS().isPresent() ? this.graphNS().get() : null;
        }
        
        IgniteConnect$() {           
            this.instance = Optional.empty();
            this.graphNS = Optional.empty();
        }
        
        public static final IgniteConnect$ MODULE$ = new IgniteConnect$();
    }

    
    
    public static String namespace() {
        return IgniteConnect$.MODULE$.namespace();
    }
    
    public static IgniteConnect getInstance(final String namespace) {
        return IgniteConnect$.MODULE$.getInstance(namespace);
    }
    
    public String graphNS() {
        return this.graphNS;
    }
    
    private Ignite ignite() {
        return this.ignite;
    }
    
    private void ignite_$eq(final Ignite x$1) {
        this.ignite = x$1;
    }
    
    public Ignite getIgnite() {
        return this.ignite();
    }
    
    public Ignite getOrCreate() {
        if (this.ignite() == null) {
            this.ignite_$eq(this.getOrStart());
        }
        return this.ignite();
    }
    
    public IgniteCache<String, BinaryObject> getOrCreateCache(final String name) {
        final IgniteCache<String, BinaryObject> cache = IgniteUtil.MODULE$.getOrCreateCache(this.ignite(), name, this.graphNS());
        if (cache == null) {
            throw new RuntimeException("Connection to Ignited failed. Could not create cache.");
        }
        return (IgniteCache<String, BinaryObject>)cache;
    }
    
    public boolean cacheExists(final String cacheName) {
        return this.ignite().cacheNames().contains(cacheName);
    }
    
    public void deleteCache(final String cacheName) {
        final boolean exists = this.ignite().cacheNames().contains(cacheName);
        if (exists) {
            this.ignite().cache(cacheName).destroy();
        }
    }
    
    public Ignite getOrStart() {
        return Ignition.getOrStart(IgniteConf.fromConfig());
    }
    
    public IgniteConnect(final String graphNS) {
        this.graphNS = graphNS;
        this.ignite = this.getOrStart();
    }
}
