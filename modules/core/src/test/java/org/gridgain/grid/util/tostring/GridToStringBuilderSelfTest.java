/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.tostring;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.locks.*;

/**
 * Tests for {@link GridToStringBuilder}.
 */
@GridCommonTest(group = "Utils")
public class GridToStringBuilderSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testToString() throws Exception {
        TestClass1 obj = new TestClass1();

        IgniteLogger log = log();

        log.info(obj.toStringManual());
        log.info(obj.toStringAutomatic());

        assert obj.toStringManual().equals(obj.toStringAutomatic());
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringWithAdditions() throws Exception {
        TestClass1 obj = new TestClass1();

        IgniteLogger log = log();

        log.info(obj.toStringWithAdditional());
    }

    /**
     * @throws Exception If failed.
     */
    public void testToStringCheckSimpleRecursionPrevention() throws Exception {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);


        GridToStringBuilder.toString(ArrayList.class, list1);
        GridToStringBuilder.toString(ArrayList.class, list2);
    }

    /**
     * @throws Exception If failed.
     */
    // TODO: Activate test when ticket GG-5000 will be ready.
    public void _testToStringCheckAdvancedRecursionPrevention() throws Exception {
        ArrayList<Object> list1 = new ArrayList<>();
        ArrayList<Object> list2 = new ArrayList<>();

        list2.add(list1);
        list1.add(list2);

        GridToStringBuilder.toString(ArrayList.class, list1, "name", list2);
        GridToStringBuilder.toString(ArrayList.class, list2, "name", list1);
    }

    /**
     * JUnit.
     */
    public void testToStringPerformance() {
        TestClass1 obj = new TestClass1();

        IgniteLogger log = log();

        // Warm up.
        obj.toStringAutomatic();

        long start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++)
            obj.toStringManual();

        log.info("Manual toString() took: " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++)
            obj.toStringAutomatic();

        log.info("Automatic toString() took: " + (System.currentTimeMillis() - start) + "ms");
    }

    /**
     * Test class.
     */
    private class TestClass1 {
        /** */
        @SuppressWarnings("unused")
        @GridToStringOrder(0)
        private String id = "1234567890";

        /** */
        @SuppressWarnings("unused")
        private int intVar;

        /** */
        @SuppressWarnings("unused")
        private long longVar;

        /** */
        @SuppressWarnings("unused")
        @GridToStringOrder(1)
        private final UUID uuidVar = UUID.randomUUID();

        /** */
        @SuppressWarnings("unused")
        private boolean boolVar;

        /** */
        @SuppressWarnings("unused")
        private byte byteVar;

        /** */
        @SuppressWarnings("unused")
        private String name = "qwertyuiopasdfghjklzxcvbnm";

        /** */
        @SuppressWarnings("unused")
        private final Integer finalInt = 2;

        /** */
        @SuppressWarnings("unused")
        private List<String> strList;

        /** */
        @SuppressWarnings("unused")
        @GridToStringInclude
        private Map<String, String> strMap;

        /** */
        @SuppressWarnings("unused")
        private final Object obj = new Object();

        /** */
        @SuppressWarnings("unused")
        private ReadWriteLock lock;

        /**
         * @return Manual string.
         */
        String toStringManual() {
            StringBuilder buf = new StringBuilder();

            buf.append(getClass().getSimpleName()).append(" [");

            buf.append("id=").append(id).append(", ");
            buf.append("uuidVar=").append(uuidVar).append(", ");
            buf.append("intVar=").append(intVar).append(", ");
            buf.append("longVar=").append(longVar).append(", ");
            buf.append("boolVar=").append(boolVar).append(", ");
            buf.append("byteVar=").append(byteVar).append(", ");
            buf.append("name=").append(name).append(", ");
            buf.append("finalInt=").append(finalInt).append(", ");
            buf.append("strMap=").append(strMap);

            buf.append("]");

            return buf.toString();
        }

        /**
         * @return Automatic string.
         */
        String toStringAutomatic() {
            return S.toString(TestClass1.class, this);
        }

        /**
         * @return String with additional parameters.
         */
        String toStringWithAdditional() {
            return S.toString(TestClass1.class, this, "newParam1", 1, "newParam2", 2);
        }
    }
}
