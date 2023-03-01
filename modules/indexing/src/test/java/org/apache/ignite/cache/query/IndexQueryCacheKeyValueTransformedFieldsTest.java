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

package org.apache.ignite.cache.query;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transform.TestCacheObjectTransformerManagerAdapter;
import org.apache.ignite.internal.processors.cache.transform.TestCacheObjectTransformerPluginProvider;

/** Test checks that indexing works (including inlining) with enabled cache objects transformer. */
public class IndexQueryCacheKeyValueTransformedFieldsTest extends IndexQueryCacheKeyValueFieldsTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName).setPluginProviders(
            new TestCacheObjectTransformerPluginProvider(new RandomShiftCacheObjectTransformer()));
    }

    /**
     * Transforms each object with a random shift.
     */
    protected static final class RandomShiftCacheObjectTransformer extends TestCacheObjectTransformerManagerAdapter {
        /** {@inheritDoc} */
        @Override public ByteBuffer transform(ByteBuffer original) {
            ByteBuffer transformed = ByteBuffer.wrap(new byte[original.remaining() + 4]);

            int shift = ThreadLocalRandom.current().nextInt();

            transformed.putInt(shift);

            while (original.hasRemaining())
                transformed.put((byte)(original.get() + shift));

            transformed.flip();

            return transformed;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer restore(ByteBuffer transformed) {
            ByteBuffer restored = ByteBuffer.wrap(new byte[transformed.remaining() - 4]);

            int shift = transformed.getInt();

            while (transformed.hasRemaining())
                restored.put((byte)(transformed.get() - shift));

            restored.flip();

            return restored;
        }
    }
}
