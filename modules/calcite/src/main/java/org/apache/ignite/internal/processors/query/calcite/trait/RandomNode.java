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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** */
public final class RandomNode implements Destination {
    /** */
    private final Random random;

    /** */
    private final List<UUID> nodes;

    /** */
    public RandomNode(List<UUID> nodes) {
        this.nodes = nodes;

        random = new Random();
    }

    /** {@inheritDoc} */
    @Override public List<UUID> targets(Object row) {
        return Collections.singletonList(nodes.get(random.nextInt(nodes.size())));
    }

    /** {@inheritDoc} */
    @Override public List<UUID> targets() {
        return nodes;
    }
}
