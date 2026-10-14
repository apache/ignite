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

package org.apache.ignite.internal.processors.rollingupgrade.message;

import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;

/** */
public interface TestMessage {
    /** */
    String A = "A";

    /** */
    String B = "B";

    /** */
    String C = "C";

    /** */
    String D = "D";

    /** */
    String E = "E";

    /** */
    String F = "F";

    /**
     * Fills the message with data. The implementation must take into account the final feature state of the release this
     * message belongs to. This should imitate how Ignite processors fill messages with RU in mind (e.g. if the feature that
     * deprecated a field is active, the field is not filled).
     *
     * @param featureStatusProvider Tells whether a feature is active in the cluster.
     */
    void fill(Predicate<IgniteFeature> featureStatusProvider);

    /** */
    default String fldA() {
        return null;
    }

    /** */
    default String fldB() {
        return null;
    }

    /** */
    default String fldC() {
        return null;
    }

    /** */
    default String fldD() {
        return null;
    }

    /** */
    default String fldE() {
        return null;
    }

    /** */
    default String fldF() {
        return null;
    }

    /** */
    default List<TestMessage> nestedMessages() {
        return List.of();
    }
}
