package org.apache.ignite.internal.processor.security.cache;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractContextResolverSecurityProcessorTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.stream.StreamVisitor;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for IgniteDataStream receiver.
 */
public class IgniteDataStreamerTest extends AbstractContextResolverSecurityProcessorTest {
    /** */
    public void testDataStreamer() {
        successReceiver(clntAllPerms, srvAllPerms);
        successReceiver(clntAllPerms, srvReadOnlyPerm);
        successReceiver(srvAllPerms, srvAllPerms);
        successReceiver(srvAllPerms, srvReadOnlyPerm);

        failReceiver(clntReadOnlyPerm, srvAllPerms);
        failReceiver(srvReadOnlyPerm, srvAllPerms);
        failReceiver(srvReadOnlyPerm, srvReadOnlyPerm);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void failReceiver(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () -> {
                    try (IgniteDataStreamer<Integer, Integer> strm = initiator.dataStreamer(CACHE_WITHOUT_PERMS)) {
                        strm.receiver(
                            StreamVisitor.from(
                                new TestClosure(remote.localNode().id(), "fail_key", -1)
                            ));

                        strm.addData(primaryKey(remote), 100);
                    }
                }
                , SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_WITH_PERMS).get("fail_key"), nullValue());
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void successReceiver(IgniteEx initiator, IgniteEx remote) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        try (IgniteDataStreamer<Integer, Integer> strm = initiator.dataStreamer(CACHE_WITHOUT_PERMS)) {
            strm.receiver(
                StreamVisitor.from(
                    new TestClosure(remote.localNode().id(), "key", val)
                ));

            strm.addData(primaryKey(remote), 100);
        }

        assertThat(remote.cache(CACHE_WITH_PERMS).get("key"), is(val));
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements
        IgniteBiInClosure<IgniteCache<Integer, Integer>, Map.Entry<Integer, Integer>> {
        /** Remote node id. */
        private final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /**
         * @param remoteId Remote node id.
         * @param key Key.
         * @param val Value.
         */
        public TestClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteCache<Integer, Integer> entries,
            Map.Entry<Integer, Integer> entry) {
            Ignite loc = Ignition.localIgnite();

            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_WITH_PERMS).put(key, val);
        }
    }
}
