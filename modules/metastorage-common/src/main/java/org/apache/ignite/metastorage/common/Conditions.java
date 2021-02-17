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

package org.apache.ignite.metastorage.common;

/**
 * This class contains fabric methods which produce conditions needed for conditional multi update functionality
 * provided by meta storage service.
 *
 * @see Condition
 */
public final class Conditions {
    /**
     * Creates condition on entry revision.
     *
     * @return Condition on entry revision.
     * @see Condition.RevisionCondition
     */
    public static Condition.RevisionCondition revision() {
        return new Condition.RevisionCondition();
    }

    /**
     * Creates condition on entry value.
     *
     * @return Condition on entry value.
     * @see Condition.ValueCondition
     */
    public static Condition.ValueCondition value() {
        return new Condition.ValueCondition();
    }

    /**
     * Default no-op constructor.
     */
    private Conditions() {
        // No-op.
    }
}
