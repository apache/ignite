/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@GridCommonTest(group = "Lang")
public class GridMetadataAwareAdapterSelfTest extends GridCommonAbstractTest {
    /** Creates test. */
    public GridMetadataAwareAdapterSelfTest() {
        super(/*start grid*/false);
    }

    /**
     * Junit.
     */
    @SuppressWarnings({"AssertWithSideEffects"})
    public void test() {
        GridMetadataAwareAdapter ma = new GridMetadataAwareAdapter();

        // addMeta(name, val).
        assert ma.addMeta("attr1", "val1") == null;
        assert ma.addMeta("attr2", 1) == null;

        // hasMeta(name).
        assert ma.hasMeta("attr1");
        assert !ma.hasMeta("attr3");

        // hasMeta(name, val).
        assert ma.hasMeta("attr1", "val1");
        assert !ma.hasMeta("attr1", "some another val");

        // meta(name).
        assertEquals("val1", ma.meta("attr1"));
        assertEquals(1, ma.meta("attr2"));

        // allMeta().
        Map<String, Object> allMeta = ma.allMeta();

        assert allMeta != null;
        assert allMeta.size() == 2;

        assertEquals("val1", allMeta.get("attr1"));
        assertEquals(1, allMeta.get("attr2"));

        // addMetaIfAbsent(name, val).
        assert ma.addMetaIfAbsent("attr2", 2) == 1;
        assert ma.addMetaIfAbsent("attr3", 3) == 3;

        // addMetaIfAbsent(name, c).
        assert ma.addMetaIfAbsent("attr2", new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return 5;
            }
        }) == 1;

        assert ma.addMetaIfAbsent("attr4", new Callable<Integer>() {
            @Override public Integer call() throws Exception {
                return 5;
            }
        }) == 5;

        // removeMeta(name).
        assertEquals(3, ma.removeMeta("attr3"));
        assertEquals(5, ma.removeMeta("attr4"));

        assert ma.removeMeta("attr156") == null;

        // replaceMeta(name, newVal, curVal).
        assert !ma.replaceMeta("attr2", 5, 4);
        assert ma.replaceMeta("attr2", 1, 4);

        // copyMeta(from).
        ma.copyMeta(new GridMetadataAwareAdapter(F.<String, Object>asMap("k1", "v1", "k2", 2)));

        assertEquals("v1", ma.meta("k1"));
        assertEquals(2, ma.meta("k2"));
        assertEquals("val1", allMeta.get("attr1"));
        assertEquals(4, allMeta.get("attr2"));

        assert !ma.hasMeta("k3");

        // copyMeta(from).
        ma.copyMeta(F.asMap("1", 1, "2", 2));

        assertEquals(1, ma.meta("1"));
        assertEquals(2, ma.meta("2"));
        assertEquals("v1", ma.meta("k1"));
        assertEquals(2, ma.meta("k2"));
        assertEquals("val1", allMeta.get("attr1"));
        assertEquals(4, allMeta.get("attr2"));

        assert !ma.hasMeta("3");
    }
}
