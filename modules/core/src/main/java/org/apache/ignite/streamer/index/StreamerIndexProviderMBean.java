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

package org.apache.ignite.streamer.index;

import org.apache.ignite.mbean.*;
import org.jetbrains.annotations.*;

/**
 * Streamer window index provider MBean.
 */
public interface StreamerIndexProviderMBean {
    /**
     * Index name.
     *
     * @return Index name.
     */
    @IgniteMBeanDescription("Index name.")
    @Nullable public String name();

    /**
     * Gets index updater class name.
     *
     * @return Index updater class.
     */
    @IgniteMBeanDescription("Index updater class name.")
    public String updaterClass();

    /**
     * Gets index unique flag.
     *
     * @return Index unique flag.
     */
    @IgniteMBeanDescription("Index unique flag.")
    public boolean unique();

    /**
     * Returns {@code true} if index supports sorting and therefore can perform range operations.
     *
     * @return Index sorted flag.
     */
    @IgniteMBeanDescription("Index sorted flag.")
    public boolean sorted();

    /**
     * Gets index policy.
     *
     * @return Index policy.
     */
    @IgniteMBeanDescription("Index policy.")
    public StreamerIndexPolicy policy();

    /**
     * Gets current index size.
     *
     * @return Current index size.
     */
    @IgniteMBeanDescription("Current index size.")
    public int size();
}
