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

import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * {@code INIT} part of a distributed DDL operation.
 */
public class DdlOperationInit implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Arguments. */
    private DdlOperationArguments args;

    /** Type. */
    private DdlOperationType opType;

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** */
    public DdlOperationArguments getArguments() {
        return args;
    }

    /** */
    public void setArguments(DdlOperationArguments args) {
        this.args = args;
    }

    /** */
    public DdlOperationType getOperationType() {
        return opType;
    }

    /** */
    public void setOperationType(DdlOperationType opType) {
        this.opType = opType;
    }
}
