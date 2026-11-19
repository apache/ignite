/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.cdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.cdc.CdcEventImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_17_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_18_0;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Checks that a {@link CdcEvent} serialized with the Java serialization by one Ignite version can be deserialized by another.
 */
@RunWith(Parameterized.class)
public class CdcEventSerializationCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Prefix of the names of the events that contain binary objects. */
    private static final String INCOMPATIBLE_DATA = "binaryObject";

    /** Superclass of the binary object implementation, the class had no explicit {@code serialVersionUID} in 2.17. */
    private static final String INCOMPATIBLE_CLS = "org.apache.ignite.internal.binary.BinaryObjectExImpl";

    /** Old Ignite version. */
    @Parameterized.Parameter
    public String oldVer;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "oldVer={0}")
    public static Collection<String> versions() {
        List<String> vers = new ArrayList<>();

        vers.add(VER_2_17_0.toString());
        vers.addAll(IgniteReleasedVersion.since(VER_2_18_0));

        return vers;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Events written by the old version and read by the current one. */
    @Test
    public void testOldEventReadByCurrent() throws Exception {
        startGrid(
            1,
            oldVer,
            cfg -> {/* No-op. */},
            ign -> ign.getOrCreateCache(DEFAULT_CACHE_NAME).putAll(serializedEvents(ign))
        );

        Map<String, byte[]> oldNodeEvts = new LinkedHashMap<>();

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            ClientCache<String, byte[]> cache = cli.cache(DEFAULT_CACHE_NAME);

            for (Cache.Entry<String, byte[]> e : cache.query(new ScanQuery<String, byte[]>()).getAll())
                oldNodeEvts.put(e.getKey(), e.getValue());
        }

        stopAllGrids();

        deserializeAndCheck(startGrid(0), oldNodeEvts, incompatible(oldVer));
    }

    /** Events written by the current version and read by the old one. */
    @Test
    public void testCurrentEventReadByOld() throws Exception {
        Map<String, byte[]> curNodeEvts = serializedEvents(startGrid(0));

        stopAllGrids();

        boolean incompatible = incompatible(oldVer);

        startGrid(
            1,
            oldVer,
            cfg -> {/* No-op. */},
            ign -> deserializeAndCheck(ign, curNodeEvts, incompatible)
        );
    }

    /** */
    private static boolean incompatible(String ver) {
        return IgniteReleasedVersion.fromString(ver).version().compareTo(VER_2_18_0.version()) < 0;
    }

    /** */
    private static void deserializeAndCheck(Ignite ign, Map<String, byte[]> serializedEvts, boolean incompatible) {
        String oldName = U.setCurrentIgniteName(ign.name());

        try {
            for (Map.Entry<String, CdcEvent> e : events(ign).entrySet()) {
                String name = e.getKey();
                byte[] bytes = serializedEvts.get(name);

                assertNotNull(name, bytes);

                if (incompatible && name.startsWith(INCOMPATIBLE_DATA))
                    assertThrows(null, () -> read(bytes), InvalidClassException.class, INCOMPATIBLE_CLS);
                else
                    assertTrue(name, same(e.getValue(), read(bytes)));
            }
        }
        catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            U.restoreOldIgniteName(oldName, ign.name());
        }
    }

    /** */
    private static Map<String, CdcEvent> events(Ignite ign) {
        GridCacheVersion ver = new GridCacheVersion(1, 2L, 3, 4);
        GridCacheVersion conflictVer = new GridCacheVersionEx(1, 3, 5L, new GridCacheVersion(1, 2L, 3, 5));

        BinaryObject key = ign.binary().builder("CdcTestKey")
            .setField("id", 1)
            .build();

        BinaryObject val = ign.binary().builder("CdcTestValue")
            .setField("name", "Sarah Connor")
            .setField("year", 1984L)
            .build();

        int cacheId = CU.cacheId(DEFAULT_CACHE_NAME);

        Map<String, CdcEvent> evts = new LinkedHashMap<>();

        evts.put("1", new CdcEventImpl(1, "val", true, 1, ver, cacheId, 0));
        evts.put("2", new CdcEventImpl(2, "val", false, 2, conflictVer, cacheId, 0));
        evts.put(INCOMPATIBLE_DATA + "3", new CdcEventImpl(key, val, true, 3, ver, cacheId, 100));
        evts.put(INCOMPATIBLE_DATA + "4", new CdcEventImpl(key, null, true, 3, ver, cacheId, 0));

        return evts;
    }

    /** Serializes events with the Java serialization, the same as {@code IgniteUtils#toBytes(Serializable)} does. */
    private static Map<String, byte[]> serializedEvents(Ignite ign) {
        Map<String, byte[]> res = new LinkedHashMap<>();

        try {
            for (Map.Entry<String, CdcEvent> e : events(ign).entrySet()) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();

                try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(e.getValue());
                }

                res.put(e.getKey(), bos.toByteArray());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    /** */
    private static CdcEvent read(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return (CdcEvent)is.readObject();
        }
    }

    /** */
    private static boolean same(CdcEvent exp, CdcEvent act) {
        return Objects.equals(exp.key(), act.key())
            && Objects.equals(exp.value(), act.value())
            && exp.primary() == act.primary()
            && exp.partition() == act.partition()
            && Objects.equals(exp.version(), act.version())
            && Objects.equals(exp.version().otherClusterVersion(), act.version().otherClusterVersion())
            && exp.cacheId() == act.cacheId()
            && exp.expireTime() == act.expireTime();
    }
}
