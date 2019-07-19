package org.apache.ignite.internal.processors.security.impl;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiInClosure;

/** . */
public class TestStoreFactory implements Factory<TestStoreFactory.TestCacheStore> {
    /** . */
    private final T2<Object, Object> keyVal;

    /** . */
    public TestStoreFactory(Object key, Object val) {
        keyVal = new T2<>(key, val);
    }

    /** {@inheritDoc} */
    @Override public TestCacheStore create() {
        return new TestCacheStore();
    }

    /** . */
    class TestCacheStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            clo.apply(keyVal.getKey(), keyVal.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }

}
