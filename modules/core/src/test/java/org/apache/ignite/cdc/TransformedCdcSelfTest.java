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

package org.apache.ignite.cdc;

import java.nio.ByteBuffer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transform.TestCacheObjectTransformerPluginProvider;
import org.apache.ignite.internal.processors.cache.transform.TestCacheObjectTransformerProcessorAdapter;

import static org.apache.ignite.internal.binary.GridBinaryMarshaller.TRANSFORMED;

/**
 *
 */
public class TransformedCdcSelfTest extends CdcSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPluginProviders(
            new TestCacheObjectTransformerPluginProvider(new CacheObjectShiftTransformer()));
    }

    /**
     * Transforms each object with a shift.
     */
    private static final class CacheObjectShiftTransformer extends TestCacheObjectTransformerProcessorAdapter {
        /** {@inheritDoc} */
        @Override public ByteBuffer transform(ByteBuffer original) {
            ByteBuffer transformed = ByteBuffer.wrap(new byte[original.remaining() + 1]);

            transformed.put(TRANSFORMED);

            while (original.hasRemaining())
                transformed.put((byte)(original.get() + 42));

            transformed.flip();

            return transformed;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer restore(ByteBuffer transformed) {
            ByteBuffer restored = ByteBuffer.wrap(new byte[transformed.remaining()]);

            while (transformed.hasRemaining())
                restored.put((byte)(transformed.get() - 42));

            restored.flip();

            return restored;
        }
    }
}
