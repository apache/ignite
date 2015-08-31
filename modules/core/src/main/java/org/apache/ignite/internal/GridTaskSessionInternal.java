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

package org.apache.ignite.internal;

import java.util.UUID;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Internal task session interface.
 */
public interface GridTaskSessionInternal extends ComputeTaskSession {
    /**
     * @return Checkpoint SPI name.
     */
    public String getCheckpointSpi();

    /**
     * @return Job ID (possibly <tt>null</tt>).
     */
    @Nullable public IgniteUuid getJobId();

    /**
     * @return {@code True} if task node.
     */
    public boolean isTaskNode();

    /**
     * Closes session.
     */
    public void onClosed();

    /**
     * @return Checks if session is closed.
     */
    public boolean isClosed();

    /**
     * @return Task session.
     */
    public GridTaskSessionInternal session();

    /**
     * @return {@code True} if checkpoints and attributes are enabled.
     */
    public boolean isFullSupport();

    /**
     * @return Subject ID.
     */
    public UUID subjectId();
}