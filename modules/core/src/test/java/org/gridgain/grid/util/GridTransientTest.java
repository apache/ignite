/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.testframework.junits.common.*;
import java.io.*;

/**
 * Transient value serialization test.
 */
@GridCommonTest(group = "Utils")
public class GridTransientTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String VALUE = "value";

    /** */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private final transient String data1;

    /** */
    @SuppressWarnings({"TransientFieldNotInitialized"})
    private final transient String data2 = VALUE;

    /** */
    public GridTransientTest() {
        data1 = VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransientSerialization() throws Exception {
        GridTransientTest objSrc = new GridTransientTest();

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        ObjectOutputStream objOut = new ObjectOutputStream(byteOut);

        objOut.writeObject(objSrc);

        objOut.close();

        ObjectInputStream objIn = new ObjectInputStream(new ByteArrayInputStream(byteOut.toByteArray()));

        GridTransientTest objDest = (GridTransientTest)objIn.readObject();

        System.out.println("Data1 value: " + objDest.data1);
        System.out.println("Data2 value: " + objDest.data2);

        assertEquals(objDest.data1, null);
        assertEquals(objDest.data2, VALUE);
    }
}
