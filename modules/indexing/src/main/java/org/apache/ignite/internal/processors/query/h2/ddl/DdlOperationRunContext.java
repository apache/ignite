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

package org.apache.ignite.internal.processors.query.h2.ddl;

import java.util.concurrent.CountDownLatch;

/**
 * Operation run context on the <b>coordinator</b>.
 */
public class DdlOperationRunContext {
    /** Latch to expect all nodes to finish their local jobs for this operation. */
    public final CountDownLatch latch;

    /** Cancellation flag. */
    public volatile boolean isCancelled = false;

    /**
     * @param nodesCnt Nodes count to initialize latch with.
     */
    public DdlOperationRunContext(int nodesCnt) {
        this.latch = new CountDownLatch(nodesCnt);
    }
}
