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

package org.apache.ignite.raft.client.message.impl;

import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.message.ActionRequest;

/** */
class ActionRequestImpl<T> implements ActionRequest, ActionRequest.Builder {
    /** */
    private String groupId;

    /** */
    private Command cmd;

    /** */
    private boolean readOnlySafe;

    /** {@inheritDoc} */
    @Override public String groupId() {
        return groupId;
    }

    /** {@inheritDoc} */
    @Override public Command command() {
        return cmd;
    }

    @Override public boolean readOnlySafe() {
        return readOnlySafe;
    }

    /** {@inheritDoc} */
    @Override public Builder groupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder command(Command cmd) {
        this.cmd = cmd;

        return this;
    }

    /** {@inheritDoc} */
    @Override public Builder readOnlySafe(boolean readOnlySafe) {
        this.readOnlySafe = readOnlySafe;

        return this;
    }

    /** {@inheritDoc} */
    @Override public ActionRequest build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 1000;
    }
}
