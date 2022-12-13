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

package org.apache.ignite.internal.processors.cache.objects;

import java.nio.ByteBuffer;
import java.util.Objects;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.spi.transform.CacheObjectsTransformer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transform.AbstractCacheObjectsTransformationTest;

/**
 *
 */
public abstract class AbstractCacheObjectsCompressionTest extends AbstractCacheObjectsTransformationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheObjectsTransformSpi(new CacheObjectsTransformSpiAdapter() {
                @Override public CacheObjectsTransformer transformer(CacheConfiguration<?, ?> ccfg) {
                    return new ZstdCompressionTransformer();
                }
            });
    }

    /**
     *
     */
    protected static final class StringData {
        /** S. */
        private final String s;

        /**
         * @param s S.
         */
        public StringData(String s) {
            this.s = s;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            StringData data = (StringData)o;

            return Objects.equals(s, data.s);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(s);
        }
    }

    /**
     *
     */
    protected static final class ZstdCompressionTransformer implements CacheObjectsTransformer {
        /** Fail. */
        protected static boolean fail;

        /** {@inheritDoc} */
        @Override public boolean direct() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public int transform(ByteBuffer original, ByteBuffer transformed, int overhead) throws IgniteCheckedException {
            if (fail)
                throw new IgniteCheckedException("Failed.");

            if (transformed.capacity() < original.remaining())
                return original.remaining();

            transformed.limit(Math.max(original.remaining() - overhead, 0)); // Limiting to gain compression profit.

            try {
                Zstd.compress(transformed, original, 1);
            }
            catch (ZstdException ex) {
                throw new IgniteCheckedException(ex);
            }

            return 0;
        }

        /** {@inheritDoc} */
        @Override public void restore(ByteBuffer transformed, ByteBuffer restored) {
            Zstd.decompress(restored, transformed);
        }
    }
}
