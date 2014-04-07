/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller.protobuf;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.util.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Protobuf marshaller self test.
 */
@SuppressWarnings("deprecation")
public class GridClientProtobufMarshallerSelfTest extends GridCommonAbstractTest {
    /**
     * Test UUID conversions from string to binary and back.
     *
     * @throws Exception On any IO exception.
     */
    public void testUuidConvertions() throws Exception {
        GridClientProtobufMarshaller m = new GridClientProtobufMarshaller();

        Map<String, byte[]> map = new LinkedHashMap<>();

        map.put("2ec84557-f7c4-4a2e-aea8-251eb13acff3", new byte[] {
            46, -56, 69, 87, -9, -60, 74, 46, -82, -88, 37, 30, -79, 58, -49, -13
        });
        map.put("4e17b7b5-79e7-4db5-ac45-a644ead95b9e", new byte[] {
            78, 23, -73, -75, 121, -25, 77, -75, -84, 69, -90, 68, -22, -39, 91, -98
        });
        map.put("412daadb-e9e6-443b-8b87-8d7895fc2e53", new byte[] {
            65, 45, -86, -37, -23, -26, 68, 59, -117, -121, -115, 120, -107, -4, 46, 83
        });
        map.put("e71aabf4-4aad-4280-b4e9-3c310be0cb88", new byte[] {
            -25, 26, -85, -12, 74, -83, 66, -128, -76, -23, 60, 49, 11, -32, -53, -120
        });
        map.put("d4454cda-a81f-490f-9424-9bdfcc9cf610", new byte[] {
            -44, 69, 76, -38, -88, 31, 73, 15, -108, 36, -101, -33, -52, -100, -10, 16
        });
        map.put("3a584450-5e85-4b69-9f9d-043d89fef23b", new byte[] {
            58, 88, 68, 80, 94, -123, 75, 105, -97, -99, 4, 61, -119, -2, -14, 59
        });
        map.put("6c8baaec-f173-4a60-b566-240a87d7f81d", new byte[] {
            108, -117, -86, -20, -15, 115, 74, 96, -75, 102, 36, 10, -121, -41, -8, 29
        });
        map.put("d99c7102-79f7-4fb4-a665-d331cf285c20", new byte[] {
            -39, -100, 113, 2, 121, -9, 79, -76, -90, 101, -45, 49, -49, 40, 92, 32
        });
        map.put("007d56c7-5c8b-4279-a700-7f3f95946dde", new byte[] {
            0, 125, 86, -57, 92, -117, 66, 121, -89, 0, 127, 63, -107, -108, 109, -34
        });
        map.put("15627963-d8f9-4423-bedc-f6f89f7d3433", new byte[] {
            21, 98, 121, 99, -40, -7, 68, 35, -66, -36, -10, -8, -97, 125, 52, 51
        });

        for (Map.Entry<String, byte[]> e : map.entrySet()) {
            UUID str = UUID.fromString(e.getKey());
            UUID byt = GridClientByteUtils.bytesToUuid(e.getValue(), 0);

            Assert.assertEquals(str, byt);

            GridClientCacheRequest<UUID, UUID> req1 =
                new GridClientCacheRequest<>(GridClientCacheRequest.GridCacheOperation.PUT);

            req1.key(str);

            GridClientCacheRequest req2 = m.unmarshal(m.marshal(req1));

            Assert.assertEquals(req1.key(), req2.key());
            Assert.assertEquals(req1.clientId(), req2.clientId());
        }
    }
}
