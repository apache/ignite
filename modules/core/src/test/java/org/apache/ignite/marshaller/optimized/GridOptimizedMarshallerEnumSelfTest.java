/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller.optimized;

import junit.framework.*;
import org.apache.ignite.marshaller.optimized.*;

/**
 *
 */
public class GridOptimizedMarshallerEnumSelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testEnumSerialisation() throws Exception {
        GridOptimizedMarshaller marsh = new GridOptimizedMarshaller();

        byte[] bytes = marsh.marshal(TestEnum.Bond);

        TestEnum unmarshalled = marsh.unmarshal(bytes, Thread.currentThread().getContextClassLoader());

        assertEquals(TestEnum.Bond, unmarshalled);
        assertEquals(TestEnum.Bond.desc, unmarshalled.desc);
    }

    private enum TestEnum {
        Equity("Equity") {
            @Override public String getTestString() {
                return "eee";
            }
        },

        Bond("Bond") {
            @Override public String getTestString() {
                return "qqq";
            }
        };

        public final String desc;

        TestEnum(String desc) {
            this.desc = desc;
        }

        public abstract String getTestString();
    }

}
