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

package org.apache.ignite.internal.processors.database;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *
 */
public class IgniteDbMemoryLeakWithExpirationTest extends IgniteDbMemoryLeakTest {
    /** */
    private static final ExpiryPolicy EXPIRY = new CreatedExpiryPolicy(new Duration(MILLISECONDS, 10L));

    /** {@inheritDoc} */
    @Override protected IgniteCache<Object, Object> cache(IgniteEx ig) {
        return ig.cache("non-primitive").withExpiryPolicy(EXPIRY);
    }

    /** {@inheritDoc} */
    @Override protected long pagesMax() {
        return 7000;
    }
}
