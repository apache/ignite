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

package org.apache.ignite.internal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.streamer.*;

import java.util.*;

/**
 * Test stage.
 */
class GridTestStage implements StreamerStage<Object> {
    /** Stage name. */
    private String name;

    /** Stage closure. */
    private SC stageClos;

    /**
     * @param name Stage name.
     * @param stageClos Stage closure to execute.
     */
    GridTestStage(String name, SC stageClos) {
        this.name = name;
        this.stageClos = stageClos;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection<Object> evts)
        throws IgniteCheckedException {
        return stageClos.apply(name(), ctx, evts);
    }
}
