package org.apache.ignite.internal.processor.security.compute;

import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractContextResolverSecurityProcessorTest;
import org.apache.ignite.internal.processor.security.TriConsumer;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.testframework.GridTestUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for distributed closure.
 */
public class DistributedClosureSecurityTest extends AbstractContextResolverSecurityProcessorTest {
    /** */
    public void testDistributedClosure() {
        checkSuccess(clntAllPerms, clntReadOnlyPerm);
        checkSuccess(clntAllPerms, srvReadOnlyPerm);
        checkSuccess(srvAllPerms, clntReadOnlyPerm);
        checkSuccess(srvAllPerms, srvReadOnlyPerm);
        checkSuccess(srvAllPerms, srvAllPerms);
        checkSuccess(clntAllPerms, clntAllPerms);

        checkFail(clntReadOnlyPerm, srvAllPerms);
        checkFail(clntReadOnlyPerm, clntAllPerms);
        checkFail(srvReadOnlyPerm, srvAllPerms);
        checkFail(srvReadOnlyPerm, clntAllPerms);
        checkFail(srvReadOnlyPerm, srvReadOnlyPerm);
        checkFail(clntReadOnlyPerm, clntReadOnlyPerm);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void checkSuccess(IgniteEx initiator, IgniteEx remote) {
        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.broadcast(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            )
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.broadcastAsync(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            ).get()
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.call(
                () -> {
                    Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                    return null;
                }
            )
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.callAsync(
                () -> {
                    Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                    return null;
                }
            ).get()
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.run(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            )
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.runAsync(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            ).get()
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.apply(
                new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                        return null;
                    }
                }, new Object()
            )
        );

        successClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.applyAsync(
                new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                        return null;
                    }
                }, new Object()
            ).get()
        );
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     */
    private void checkFail(IgniteEx initiator, IgniteEx remote) {
        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.broadcast(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            )
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.broadcastAsync(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            ).get()
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.call(
                () -> {
                    Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                    return null;
                }
            )
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.callAsync(
                () -> {
                    Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                    return null;
                }
            ).get()
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.run(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            )
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.runAsync(
                () -> Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v)
            ).get()
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.apply(
                new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                        return null;
                    }
                }, new Object()
            )
        );

        failClosure(
            initiator, remote,
            (cmp, k, v) -> cmp.applyAsync(
                new IgniteClosure<Object, Object>() {
                    @Override public Object apply(Object o) {
                        Ignition.localIgnite().cache(CACHE_WITH_PERMS).put(k, v);

                        return null;
                    }
                }, new Object()
            ).get()
        );
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     * @param consumer Consumer.
     */
    private void successClosure(IgniteEx initiator, IgniteEx remote,
        TriConsumer<IgniteCompute, String, Integer> consumer) {
        int val = values.getAndIncrement();

        consumer.accept(initiator.compute(initiator.cluster().forNode(remote.localNode())), "key", val);

        assertThat(remote.cache(CACHE_WITH_PERMS).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     * @param consumer Consumer.
     */
    private void failClosure(IgniteEx initiator, IgniteEx remote,
        TriConsumer<IgniteCompute, String, Integer> consumer) {
        assertCauseMessage(
            GridTestUtils.assertThrowsWithCause(
                () ->
                    consumer.accept(
                        initiator.compute(initiator.cluster().forNode(remote.localNode())), "fail_key", -1
                    ), SecurityException.class
            )
        );

        assertThat(remote.cache(CACHE_WITH_PERMS).get("fail_key"), nullValue());
    }
}
