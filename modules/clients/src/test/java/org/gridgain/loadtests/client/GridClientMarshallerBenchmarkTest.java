/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.client;

import org.gridgain.client.marshaller.*;
import org.gridgain.client.marshaller.jdk.*;
import org.gridgain.client.marshaller.optimized.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.client.message.GridClientCacheRequest.GridCacheOperation.*;

/**
 * Tests basic performance of marshallers.
 */
public class GridClientMarshallerBenchmarkTest extends GridCommonAbstractTest {
    /** Marshallers to test. */
    private GridClientMarshaller[] marshallers;

    /**
     */
    public GridClientMarshallerBenchmarkTest() {
        marshallers = new GridClientMarshaller[] {
            new GridClientJdkMarshaller(),
            new GridClientOptimizedMarshaller()
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheRequestTime() throws Exception {
        GridClientCacheRequest req = new GridClientCacheRequest(CAS);

        req.clientId(UUID.randomUUID());
        req.cacheName("CacheName");
        req.requestId(1024);
        req.key("key");
        req.value(1L);
        req.value2(2L);

        Map<Object, Object> additional = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            additional.put("key" + i, (long)i);

        req.values(additional);

        // Warm up.
        for (GridClientMarshaller marshaller : marshallers) {
            GridClientCacheRequest res = runMarshallUnmarshalLoop(req, 1, marshaller);

            assertEquals(req.operation(), res.operation());
            assertEquals(0, res.requestId()); // requestId is not marshalled.
            assertEquals(null, res.clientId()); // clientId is not marshalled.
            assertEquals(null, res.destinationId()); // destinationId is not marshalled.
            assertEquals(req.cacheName(), res.cacheName());
            assertEquals(req.key(), res.key());
            assertEquals(req.value(), res.value());
            assertEquals(req.value2(), res.value2());

            for (Map.Entry<Object, Object> e : req.values().entrySet())
                assertEquals(e.getValue(), res.values().get(e.getKey()));
        }

        // Now real test.
        for (GridClientMarshaller marshaller : marshallers)
            runMarshallUnmarshalLoop(req, 1000, marshaller);
    }

    /**
     * Runs marshal/unmarshal loop and prints statistics.
     *
     * @param obj Object to marshal.
     * @param iterCnt Iteration count.
     * @param marshaller Marshaller to use.
     * @throws IOException If marshalling failed.
     * @return Unmarshalled object in last iteration
     */
    @SuppressWarnings("unchecked")
    private <T> T runMarshallUnmarshalLoop(T obj, int iterCnt, GridClientMarshaller marshaller)
        throws IOException {
        if (iterCnt == 1) {
            // Warm-up, will not print statistics.
            ByteBuffer buf = marshaller.marshal(obj, 0);

            byte[] arr = new byte[buf.remaining()];

            buf.get(arr);

            Object res = marshaller.unmarshal(arr);

            assertNotNull("Failed for marshaller: " + marshaller.getClass().getSimpleName(), res);

            return (T)res;
        }

        long marshallingTime = 0, unmarshallingTime = 0;

        long start = System.currentTimeMillis();

        Object res = null;

        for (int i = 0; i < iterCnt; i++) {
            ByteBuffer buf = marshaller.marshal(obj, 0);

            byte[] raw = new byte[buf.remaining()];

            buf.get(raw);

            long end = System.currentTimeMillis();

            marshallingTime += (end - start);

            start = end;

            res = marshaller.unmarshal(raw);

            assertNotNull(res);

            end = System.currentTimeMillis();

            unmarshallingTime += (end - start);

            start = end;
        }

        X.println("Marshalling statistics gathered [marshallerClass=" + marshaller.getClass().getSimpleName() +
            ", objClass=" + obj.getClass().getSimpleName() + ", marshallingTime=" + marshallingTime +
            ", unmarshallingTime=" + unmarshallingTime + "]");

        assert res != null;

        return (T)res;
    }
}
