package org.apache.ignite.hadoop.util;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * Tests for chained user name mapper.
 */
public class ChainedUserNameMapperSelfTest extends GridCommonAbstractTest {
    /** Test instance. */
    private static final String INSTANCE = "test_instance";

    /** Test realm. */
    private static final String REALM = "test_realm";

    /**
     * Test case when mappers are null.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testNullMappers() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                create((UserNameMapper[])null);

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * Test case when one of mappers is null.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testNullMapperElement() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                create(new BasicUserNameMapper(), null);

                return null;
            }
        }, IgniteException.class, null);
    }

    /**
     * Test actual chaining logic.
     *
     * @throws Exception If failed.
     */
    public void testChaining() throws Exception {
        BasicUserNameMapper mapper1 = new BasicUserNameMapper();

        mapper1.setMappings(Collections.singletonMap("1", "101"));

        KerberosUserNameMapper mapper2 = new KerberosUserNameMapper();

        mapper2.setInstance(INSTANCE);
        mapper2.setRealm(REALM);

        ChainedUserNameMapper mapper = create(mapper1, mapper2);

        assertEquals("101" + "/" + INSTANCE + "@" + REALM, mapper.map("1"));
        assertEquals("2" + "/" + INSTANCE + "@" + REALM, mapper.map("2"));
        assertEquals(IgfsUtils.fixUserName(null) + "/" + INSTANCE + "@" + REALM, mapper.map(null));
    }

    /**
     * Create chained mapper.
     *
     * @param mappers Child mappers.
     * @return Chained mapper.
     */
    private ChainedUserNameMapper create(UserNameMapper... mappers) {
        ChainedUserNameMapper mapper = new ChainedUserNameMapper();

        mapper.setMappers(mappers);

        mapper.start();

        return mapper;
    }
}
