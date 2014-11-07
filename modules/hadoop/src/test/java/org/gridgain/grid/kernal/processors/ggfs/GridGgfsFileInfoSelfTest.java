/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * {@link org.gridgain.grid.kernal.processors.ggfs.GridGgfsFileInfo} test case.
 */
public class GridGgfsFileInfoSelfTest extends GridGgfsCommonAbstractTest {
    /** Marshaller to test {@link Externalizable} interface. */
    private final GridMarshaller marshaller = new GridOptimizedMarshaller();

    /**
     * Test node info serialization.
     *
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        final int max = Integer.MAX_VALUE;

        multithreaded(new Callable<Object>() {
            private final Random rnd = new Random();

            @SuppressWarnings("deprecation") // Suppress due to default constructor should never be used directly.
            @Nullable @Override public Object call() throws GridException {
                for (int i = 0; i < 10000; i++) {
                    testSerialization(new GridGgfsFileInfo());
                    testSerialization(new GridGgfsFileInfo());
                    testSerialization(new GridGgfsFileInfo(true, null));
                    testSerialization(new GridGgfsFileInfo(false, null));

                    GridGgfsFileInfo rndInfo = new GridGgfsFileInfo(rnd.nextInt(max), null, false, null);

                    testSerialization(rndInfo);
                    testSerialization(new GridGgfsFileInfo(rndInfo, rnd.nextInt(max)));
                    testSerialization(new GridGgfsFileInfo(rndInfo, F.asMap("desc", String.valueOf(rnd.nextLong()))));
                }

                return null;
            }
        }, 20);
    }

    /**
     * Test node info serialization.
     *
     * @param info Node info to test serialization for.
     * @throws GridException If failed.
     */
    public void testSerialization(GridGgfsFileInfo info) throws GridException {
        assertEquals(info, mu(info));
    }

    /**
     * Marshal/unmarshal object.
     *
     * @param obj Object to marshal/unmarshal.
     * @return Marshalled and then unmarshalled object.
     * @throws GridException In case of any marshalling exception.
     */
    private <T> T mu(T obj) throws GridException {
        return marshaller.unmarshal(marshaller.marshal(obj), null);
    }
}
