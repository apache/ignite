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

package org.apache.ignite.internal.managers.discovery;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class IncompleteDeserializationExceptionTest extends GridCommonAbstractTest {
    /** */
    private Path tmpDir;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        tmpDir = Files.createTempDirectory(UUID.randomUUID().toString());

        Files.createDirectories(tmpDir);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Files.delete(tmpDir);

        super.afterTest();
    }

    /** */
    @Test
    public void testMissingClassDeserialization() throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(getClass().getResourceAsStream("Wrapper.ser"))) {
            in.readObject();

            fail("Exception is expected");
        }
        catch (IncompleteDeserializationException e) {
            Wrapper wrp = (Wrapper)e.message();

            assertNotNull(wrp);
            assertEquals(42, wrp.i);
            assertNull(wrp.o);
        }
    }

    /** */
    public static class Wrapper implements DiscoveryCustomMessage {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        private final int i;

        /** */
        private final Object o;

        /** */
        private Wrapper(int i, Object o) {
            this.i = i;
            this.o = o;
        }

        /** */
        private void readObject(ObjectInputStream in) throws IOException {
            try {
                in.defaultReadObject();
            }
            catch (ClassNotFoundException e) {
                throw new IncompleteDeserializationException(this);
            }
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid id() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public @Nullable DiscoveryCustomMessage ackMessage() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isMutable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr, AffinityTopologyVersion topVer,
            DiscoCache discoCache) {
            return null;
        }
    }

    // Commented lines were used to prepare serialized object
//    public static void main(String[] args) throws IOException {
//        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("Wrapper.ser"))) {
//            out.writeObject(new Wrapper(42, new ForeignClass()));
//        }
//    }
//
//    public static class ForeignClass implements Serializable {
//    }
}
