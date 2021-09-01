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

package org.apache.ignite.internal.processors.cache.binary.datastreaming;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.datastreamer.DataStreamProcessorSelfTest;
import org.apache.ignite.stream.StreamReceiver;

/**
 *
 */
public class DataStreamProcessorBinarySelfTest extends DataStreamProcessorSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BinaryMarshaller marsh = new BinaryMarshaller();

        cfg.setMarshaller(marsh);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected StreamReceiver<String, TestObject> getStreamReceiver() {
        return new TestDataReceiver();
    }

    /** {@inheritDoc} */
    @Override protected boolean customKeepBinary() {
        return true;
    }

    /**
     *
     */
    private static class TestDataReceiver implements StreamReceiver<String, TestObject> {
        /** {@inheritDoc} */
        @Override public void receive(IgniteCache<String, TestObject> cache,
            Collection<Map.Entry<String, TestObject>> entries) {
            for (Map.Entry<String, TestObject> e : entries) {
                assertTrue(e.getKey() instanceof String);
                assertTrue(String.valueOf(e.getValue()), e.getValue() instanceof BinaryObject);

                TestObject obj = ((BinaryObject)e.getValue()).deserialize();

                cache.put(e.getKey(), new TestObject(obj.val + 1));
            }
        }
    }
}
