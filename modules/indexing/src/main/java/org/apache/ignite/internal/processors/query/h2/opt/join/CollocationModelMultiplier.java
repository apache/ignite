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

package org.apache.ignite.internal.processors.query.h2.opt.join;

/**
 * Multiplier for different collocation types.
 */
public enum CollocationModelMultiplier {
    /** Tables are collocated, cheap. */
    COLLOCATED(1),

    /** */
    UNICAST(50),

    /** */
    BROADCAST(200),

    /** Force REPLICATED tables to be at the end of join sequence. */
    REPLICATED_NOT_LAST(10_000);

    /** Multiplier value. */
    private final int multiplier;

    /**
     * Constructor.
     *
     * @param multiplier Multiplier value.
     */
    CollocationModelMultiplier(int multiplier) {
        this.multiplier = multiplier;
    }

    /**
     * @return Multiplier value.
     */
    public int multiplier() {
        return multiplier;
    }
}
