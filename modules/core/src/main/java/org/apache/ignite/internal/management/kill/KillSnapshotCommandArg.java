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

package org.apache.ignite.internal.management.kill;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.kill.SnapshotCancelTask.CancelSnapshotArg;

/** */
public class KillSnapshotCommandArg extends CancelSnapshotArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(value = 0)
    @Positional
    @Argument(description = "Request id")
    UUID requestId;

    /** */
    @Order(value = 1)
    String snapshotName;

    /** {@inheritDoc} */
    @Override public UUID requestId() {
        return requestId;
    }

    /** */
    public void requestId(UUID requestId) {
        this.requestId = requestId;
    }

    /** {@inheritDoc} */
    @Override public String snapshotName() {
        return snapshotName;
    }

    /** */
    public void snapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }
}
