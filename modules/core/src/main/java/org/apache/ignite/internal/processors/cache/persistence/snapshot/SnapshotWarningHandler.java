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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotHandlerType.CREATE;

/**
 * Snapshot haldler that collects and registers snapshot operation warning.
 */
public class SnapshotWarningHandler implements SnapshotHandler<List<String>> {
    /** */
    private final Consumer<Map<UUID, List<String>>> wrnsConsumer;

    /**
     * @param wrnsConsumer Where to give warnings to.
     */
    public SnapshotWarningHandler(Consumer<Map<UUID, List<String>>> wrnsConsumer) {
        this.wrnsConsumer = wrnsConsumer;
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return CREATE;
    }

    /** {@inheritDoc} */
    @Override public List<String> invoke(SnapshotHandlerContext ctx) {
        return ctx.metadata().warnings() == null ? Collections.emptyList() : ctx.metadata().warnings();
    }

    /** {@inheritDoc} */
    @Override public void complete(String name, Collection<SnapshotHandlerResult<List<String>>> results)
        throws Exception {
        Map<UUID, List<String>> wrns = new HashMap<>();

        for (SnapshotHandlerResult<List<String>> res : results) {
            if (res.data() == null || res.data().isEmpty())
                continue;

            wrns.put(res.node().id(), res.data());
        }

        if (!wrns.isEmpty())
            wrnsConsumer.accept(wrns);
    }
}
