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

package org.apache.ignite.internal.util;

import org.apache.ignite.IgniteSystemProperties;

/**
 * Temporary sets {@link IgniteSystemProperties} property to specified value.
 * Intended for use in try-with-resources.
 *
 * <p>Restores {@link IgniteSystemProperties} property
 * setting in {@link IgniteSysPropTempChanger#close()} method.
 */
public class IgniteSysPropTempChanger implements AutoCloseable {
    /** Property name */
    private final String name;
    /** Old property value, null if not set */
    private final String oldVal;

    /** Records {@link org.apache.ignite.IgniteSystemProperties} specified property
     * value to restore or clearit in {@link #close()} method. Then sets it to the new value.
     *
     * @param name The name of the property.
     * @param newVal The new property value to set. Specify {@code null} to clear property.
     */
    public IgniteSysPropTempChanger(String name, String newVal) {
        this.name = name;
        this.oldVal = System.getProperty(name);

        if (newVal != null)
            System.setProperty(name, newVal);
        else
            System.clearProperty(name);
    }

    /**
     * Restores recorded property value or clears it if it did not exist.
     */
    @Override public void close() {
        if (oldVal == null)
            System.clearProperty(name);
        else
            System.setProperty(name, oldVal);
    }
}
