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

package org.apache.ignite.configuration.internal;

import java.util.Map;

/**
 * This class holds named configurations in VIEW object.
 */
public class NamedList<T> {
    /** Named values. */
    private final Map<String, T> values;

    /**
     * Constructor.
     * @param values Named values.
     */
    public NamedList(Map<String, T> values) {
        this.values = values;
    }

    /**
     * Get named values.
     * @return Named values.
     */
    public Map<String, T> getValues() {
        return values;
    }
}
