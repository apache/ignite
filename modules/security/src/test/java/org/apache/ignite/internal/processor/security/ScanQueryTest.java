package org.apache.ignite.internal.processor.security;

import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security test for scan query.
 */
public class ScanQueryTest extends AbstractContextResolverSecurityProcessorTest {
    /** */
    public void testScanQuery() throws Exception {
        putTestData(succsessSrv, CACHE_NAME);
        putTestData(succsessSrv, SEC_CACHE_NAME);

        awaitPartitionMapExchange();

        successQuery(succsessClnt, succsessSrv, CACHE_NAME);
        successQuery(succsessSrv, succsessSrv, CACHE_NAME);
        successQuery(succsessClnt, succsessSrv, SEC_CACHE_NAME);
        successQuery(succsessSrv, succsessSrv, SEC_CACHE_NAME);

        successTransform(succsessClnt, succsessSrv, CACHE_NAME);
        successTransform(succsessSrv, succsessSrv, CACHE_NAME);
        successTransform(succsessClnt, succsessSrv, SEC_CACHE_NAME);
        successTransform(succsessSrv, succsessSrv, SEC_CACHE_NAME);

        successQuery(succsessClnt, failSrv, CACHE_NAME);
        successQuery(succsessSrv, failSrv, CACHE_NAME);
        successQuery(succsessClnt, failSrv, SEC_CACHE_NAME);
        successQuery(succsessSrv, failSrv, SEC_CACHE_NAME);

        successTransform(succsessClnt, failSrv, CACHE_NAME);
        successTransform(succsessSrv, failSrv, CACHE_NAME);
        successTransform(succsessClnt, failSrv, SEC_CACHE_NAME);
        successTransform(succsessSrv, failSrv, SEC_CACHE_NAME);

        failQuery(failClnt, succsessSrv, CACHE_NAME);
        failQuery(failSrv, succsessSrv, CACHE_NAME);
        failQuery(failClnt, succsessSrv, SEC_CACHE_NAME);
        failQuery(failSrv, succsessSrv, SEC_CACHE_NAME);

        failTransform(failClnt, succsessSrv, CACHE_NAME);
        failTransform(failSrv, succsessSrv, CACHE_NAME);
        failTransform(failClnt, succsessSrv, SEC_CACHE_NAME);
        failTransform(failSrv, succsessSrv, SEC_CACHE_NAME);
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void failQuery(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> {
                    initiator.cache(cacheName).query(
                        new ScanQuery<>(
                            new QueryFilter(remote.localNode().id(), "fail_key", -1)
                        )
                    ).getAll();
                }
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void successQuery(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        initiator.cache(cacheName).query(
            new ScanQuery<>(
                new QueryFilter(remote.localNode().id(), "key", val)
            )
        ).getAll();

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void failTransform(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> {
                    initiator.cache(cacheName).query(
                        new ScanQuery<>((k, v) -> true),
                        new Transformer(remote.localNode().id(), "fail_key", -1)
                    ).getAll();
                }
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }

    /**
     * @param initiator Initiator.
     * @param remote Remote.
     * @param cacheName Cache name.
     */
    private void successTransform(IgniteEx initiator, IgniteEx remote, String cacheName) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        initiator.cache(cacheName).query(
            new ScanQuery<>((k, v) -> true),
            new Transformer(remote.localNode().id(), "key", val)
        ).getAll();

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void putTestData(IgniteEx ignite, String cacheName) {
        try (IgniteDataStreamer<String, Integer> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 1; i <= 100; i++)
                streamer.addData(Integer.toString(i), i);
        }
    }

    /**
     * Common class for test closures.
     */
    static class CommonClosure {
        /** Remote node id. */
        protected final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public CommonClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /**
         * Put value to cache.
         */
        protected void put() {
            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(key, val);
        }
    }

    /**
     * Test query filter.
     * */
    static class QueryFilter extends CommonClosure implements IgniteBiPredicate<String, Integer> {
        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public QueryFilter(UUID remoteId, String key, Integer val) {
            super(remoteId, key, val);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(String s, Integer i) {
            put();

            return false;
        }
    }

    /**
     * Test transformer.
     */
    static class Transformer extends CommonClosure implements IgniteClosure<Cache.Entry<String, Integer>, Integer> {
        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public Transformer(UUID remoteId, String key, Integer val) {
            super(remoteId, key, val);
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<String, Integer> entry) {
            put();

            return entry.getValue();
        }
    }
}
