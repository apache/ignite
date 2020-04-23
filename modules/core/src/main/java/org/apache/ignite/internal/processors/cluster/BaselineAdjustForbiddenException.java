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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.IgniteException;

/**
 * Exception which would be throw during force changing baseline if cluster has incorrect configuration.
 */
public class BaselineAdjustForbiddenException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final String BASELINE_AUTO_ADJUST_ENABLED =
        "Baseline auto-adjust is enabled, please turn-off it before try to adjust baseline manually";

    /** */
    private static final String BASELINE_AUTO_ADJUST_DISABLED =
        "Baseline auto-adjust is disabled";

    /**
     * @param isBaselneAutoAdjustEnabled {@code true} if baseline auto-adjust enabled.
     */
    public BaselineAdjustForbiddenException(boolean isBaselneAutoAdjustEnabled) {
        super(isBaselneAutoAdjustEnabled ? BASELINE_AUTO_ADJUST_ENABLED : BASELINE_AUTO_ADJUST_DISABLED);
    }
}
