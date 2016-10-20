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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.Collections;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryIdentity;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;

/**
 *
 */
@SuppressWarnings("unchecked")
public class JmhCacheBinKeysBenchmark extends JmhCacheBenchmark {
    /** */
    private BinaryIdentity keyIdentity;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        keyIdentity = identity();

        super.setup();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration configuration(String gridName) {
        IgniteConfiguration cfg = super.configuration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setTypeConfigurations(Collections.<BinaryTypeConfiguration>singletonList(
            new BinaryTypeConfiguration() {{
                setTypeName("Int3FieldsKey");

                setIdentity(keyIdentity);
            }}
        ));

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /**
     * @return New identity to hash/compare keys with, {@code null} for default behavior.
     */
    protected BinaryIdentity identity() {
        return null;
    }

    /**
     * @param k Base value to initialize key fields upon.
     * @return Binary key having 3 fields.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override protected BinaryObject createKey(int k) {
        BinaryObjectBuilder bldr = node.binary().builder("Int3FieldsKey");

        Long l = (long) k;

        bldr.setField("f1", 1);
        bldr.setField("f2", "suffix");
        bldr.setField("f3", l);

        if (keyIdentity == null) {
            int hash = 0;

            hash = 31 * hash + 1;
            hash = 31 * hash + ("suffix".hashCode());
            hash = 31 * hash + l.hashCode();

            bldr.hashCode(hash);
        }

        return bldr.build();
    }
}
