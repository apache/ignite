package org.apache.ignite.rs;

import javax.cache.Cache;
import java.io.Serializable;


public class CacheEntryDTO<K, V> implements Cache.Entry<K, V>, Serializable {

    private K k;
    private V v;

    public CacheEntryDTO(K k, V v){
        this.k = k;
        this.v = v;
    }

    @Override
    public K getKey() {
        return k;
    }

    @Override
    public V getValue() {
        return v;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return null;
    }

    @Override
    public String toString() {
        return "[key-> " + k + ", val-> " + v + "]";
    }
}
