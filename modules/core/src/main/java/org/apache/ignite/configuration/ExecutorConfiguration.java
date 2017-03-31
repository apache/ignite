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

package org.apache.ignite.configuration;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_PUBLIC_THREAD_CNT;

/**
 * Configuration for custom thread pool that is used for user compute tasks.
 */
public class ExecutorConfiguration {
    /** Executor name. */
    private String name ;

    /** Thread pool size. */
    private int size = DFLT_PUBLIC_THREAD_CNT;

    /**
     * @return Executor name.
     */
    @NotNull public String getName() {
        return name;
    }

    /**
     * @param name Executor name.
     */
    public void setName(@NotNull String name) {
        this.name = name;
    }

    /**
     * @return Thread pool size.
     */
    public int getSize() {
        return size;
    }

    /**
     * @param size Thread pool size.
     */
    public void setSize(int size) {
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExecutorConfiguration.class, this);
    }
}