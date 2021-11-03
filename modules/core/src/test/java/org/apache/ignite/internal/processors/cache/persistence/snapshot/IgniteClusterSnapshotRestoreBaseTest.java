/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import java.util.function.Function;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.IgniteEx;
import org.junit.runners.Parameterized;

/**
 * Snapshot restore test base.
 */
public abstract class IgniteClusterSnapshotRestoreBaseTest extends AbstractSnapshotSelfTest {
    /** Parameters. Encrypted snapshots are not supported. */
    @Parameterized.Parameters(name = "Encryption is disabled")
    public static Iterable<Boolean> disabledEncryption() {
        return Collections.singletonList(false);
    }

    /**
     * @param nodesCnt Nodes count.
     * @param keysCnt Number of keys to create.
     * @return Ignite coordinator instance.
     * @throws Exception if failed.
     */
    protected IgniteEx startGridsWithSnapshot(int nodesCnt, int keysCnt) throws Exception {
        return startGridsWithSnapshot(nodesCnt, keysCnt, false);
    }

    /** */
    protected class BinaryValueBuilder implements Function<Integer, Object> {
        /** Binary type name. */
        private final String typeName;

        /**
         * @param typeName Binary type name.
         */
        BinaryValueBuilder(String typeName) {
            this.typeName = typeName;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            BinaryObjectBuilder builder = grid(0).binary().builder(typeName);

            builder.setField("id", key);
            builder.setField("name", String.valueOf(key));

            return builder.build();
        }
    }
}
