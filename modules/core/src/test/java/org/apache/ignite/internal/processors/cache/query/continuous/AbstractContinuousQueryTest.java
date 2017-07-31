package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer.TransformedEventListener;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * @author SBT-Izhikov-NV
 */
public abstract class AbstractContinuousQueryTest extends GridCommonAbstractTest {
    public abstract boolean isContinuousWithTransformer();

    protected Query createContinuousQuery() {
        if (isContinuousWithTransformer()) {
            ContinuousQueryWithTransformer<Object, Object, Object> q = new ContinuousQueryWithTransformer<>();

            q.setRemoteTransformerFactory(
                FactoryBuilder.factoryOf(new IgniteBiClosure<Object, Object, Object>() {
                    @Override public Object apply(Object key, Object val) {
                        return new T2(key, val);
                    }
                }));

            return q;
        }

        return new ContinuousQuery();
    }

    protected <K, V> void setLocalListener(Query q, CI2<Ignite, T2<K, V>> lsnrClsr) {
        if (isContinuousWithTransformer()) {
            ((ContinuousQueryWithTransformer)q)
                .setLocalTransformedEventListener(new TransformedEventListenerImpl(lsnrClsr));
        } else
            ((ContinuousQuery)q).setLocalListener(new CacheInvokeListener(lsnrClsr));
    }

    protected void setRemoteFilterFactory(Query q, Factory<? extends CacheEntryEventFilter> rmtFilterFactory) {
        if (isContinuousWithTransformer())
            ((ContinuousQueryWithTransformer)q).setRemoteFilterFactory(rmtFilterFactory);
        else
            ((ContinuousQuery)q).setRemoteFilterFactory(rmtFilterFactory);
    }

    /**
     *
     */
    @IgniteAsyncCallback
    protected static class CacheInvokeListenerAsync<K, V> extends CacheInvokeListener<K, V> {
        /**
         * @param clsr Closure.
         */
        public CacheInvokeListenerAsync(
            CI2<Ignite, T2<K, V>> clsr) {
            super(clsr);
        }
    }

    /**
     *
     */
    protected static class CacheInvokeListener<K, V> implements CacheEntryUpdatedListener<K, V>,
        CacheEntryCreatedListener<K, V>, Serializable {
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private CI2<Ignite, T2<K, V>> clsr;

        /**
         * @param clsr Closure.
         */
        public CacheInvokeListener(CI2<Ignite, T2<K, V>> clsr) {
            this.clsr = clsr;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> e : events)
                clsr.apply(ignite, new T2<>(e.getKey(), e.getValue()));
        }

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends K,
            ? extends V>> events) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> e : events)
                clsr.apply(ignite, new T2<>(e.getKey(), e.getValue()));
        }
    }

    @IgniteAsyncCallback
    protected static class TransformedEventListenerImplAsync<K, V> extends TransformedEventListenerImpl<K, V> {
        /**
         * @param clsr Closure.
         */
        public TransformedEventListenerImplAsync(CI2<Ignite, T2<K, V>> clsr) {
            super(clsr);
        }
    }

    protected static class TransformedEventListenerImpl<K, V> implements TransformedEventListener {
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteBiInClosure<Ignite, T2<K, V>> clsr;

        public TransformedEventListenerImpl(CI2<Ignite, T2<K, V>> clsr) {
            this.clsr = clsr;
        }

        @Override public void onUpdated(Iterable evts) throws CacheEntryListenerException {
            for (Object e : evts) {
                //IgniteBiTuple t2 = (IgniteBiTuple)e;
                //clsr.apply(ignite, new T2(t2.getKey(), t2.getValue()));
                clsr.apply(ignite, (T2)e);
            }
        }
    }

    /**
     *
     */
    @IgniteAsyncCallback
    protected static class CacheTestRemoteFilterAsync<K, V> extends CacheTestRemoteFilter {
        /**
         * @param clsr Closure.
         */
        public CacheTestRemoteFilterAsync(
            IgniteBiInClosure<Ignite, CacheEntryEvent<? extends K, ? extends V>> clsr) {
            super(clsr);
        }
    }

    /**
     *
     */
    protected static class CacheTestRemoteFilter<K, V> implements CacheEntryEventSerializableFilter<K, V> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private IgniteBiInClosure<Ignite, CacheEntryEvent<? extends K, ? extends V>> clsr;

        /**
         * @param clsr Closure.
         */
        public CacheTestRemoteFilter(IgniteBiInClosure<Ignite, CacheEntryEvent<? extends K, ? extends V>> clsr) {
            this.clsr = clsr;
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> e)
            throws CacheEntryListenerException {
            clsr.apply(ignite, e);

            return true;
        }
    }
}
