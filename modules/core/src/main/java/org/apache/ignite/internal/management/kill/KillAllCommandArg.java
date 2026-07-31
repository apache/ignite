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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Argument for --kill all command.
 */
public class KillAllCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Target type. */
    @Order(0)
    @Positional
    @Argument()
    @EnumDescription(
        names = {
            "SQL",
            "SCAN",
            "CONTUNUOUS"
        },
        descriptions = {
            "SQL queries",
            "SCAN queries",
            "CONTUNUOUS queries"
        }
    )
    TargetType target;

    /** Node ID to filter targets. */
    @Order(1)
    @Argument(description = "Originating node ID to filter targets", optional = true)
    UUID nodeId;

    /** Minimum duration in seconds. */
    @Order(2)
    @Argument(description = "Minimum duration in seconds", example = "60", optional = true)
    Long minDuration;

    /**
     * Target type enum.
     */
    public enum TargetType {
        /** */
        SQL,

        /** */
        SCAN,

        /** */
        CONTINUOUS
    }

    /**
     * @return Target type.
     */
    public TargetType target() {
        return target;
    }

    /**
     * @param target Target type.
     */
    public void target(TargetType target) {
        this.target = target;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Minimum duration in seconds.
     */
    public Long minDuration() {
        return minDuration;
    }

    /**
     * @param minDuration Minimum duration in seconds.
     */
    public void minDuration(Long minDuration) {
        A.ensure(minDuration == null || minDuration > 0, "--min-duration");
        A.ensure(minDuration == null || target != TargetType.CONTINUOUS,
            "--minDuration is not supported for CONTINUOUS queries");

        this.minDuration = minDuration;
    }
}
