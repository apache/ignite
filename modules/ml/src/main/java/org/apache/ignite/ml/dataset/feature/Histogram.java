/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.dataset.feature;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

/**
 * Interface of histogram over type T.
 *
 * @param <T> Type of object for histogram.
 * @param <H> Type of histogram that can be used in math operations with this histogram.
 */
public interface Histogram<T, H extends Histogram<T, H>> extends Serializable {
    /**
     * Add object to histogram.
     *
     * @param val Value.
     */
    public void addElement(T val);

    /**
     *
     * @return bucket ids.
     */
    public Set<Integer> buckets();

    /**
     *
     * @param bucketId Bucket id.
     * @return value in according to bucket id.
     */
    public Optional<Double> getValue(Integer bucketId);

    /**
     * @param other Other histogram.
     * @return sum of this and other histogram.
     */
    public H plus(H other);

    /**
     * Compares histogram with other and returns true if they are equals
     *
     * @param other Other histogram.
     * @return true if histograms are equal.
     */
    public boolean isEqualTo(H other);
}
