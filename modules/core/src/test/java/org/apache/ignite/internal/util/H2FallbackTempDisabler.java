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
 * Temporary disables fallback to H2 SQL parser in favor of the internal one.
 * Intended for use in try-with-resources.
 *
 * <p></p>Restores {@link IgniteSystemProperties#IGNITE_SQL_PARSER_H2_FALLBACK_DISABLED}
 * setting in {@link H2FallbackTempDisabler#close()} method.
 */
public class H2FallbackTempDisabler extends IgniteSysPropTempChanger {

    /** Creates temporary disabler.
     *
     * @param disable true to disable H2 SQL parser fallback, false to try both (internal, then H2 one).
     */
    public H2FallbackTempDisabler(boolean disable) {

        super(IgniteSystemProperties.IGNITE_SQL_PARSER_H2_FALLBACK_DISABLED, Boolean.toString(disable).toLowerCase());
    }
}
