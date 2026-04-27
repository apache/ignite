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

package org.apache.ignite.spi.communication;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;

/** */
public class TestMessage extends GridCacheMessage {
    /** */
    public static final short DIRECT_TYPE = 202;

    /** */
    @Order(0)
    Collection<TestMessage1> entries = new ArrayList<>();

    /** @param entry Entry. */
    public void add(TestMessage1 entry) {
        entries.add(entry);
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** @return Collection of test messages. */
    public Collection<TestMessage1> entries() {
        return entries;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE;
    }
}
