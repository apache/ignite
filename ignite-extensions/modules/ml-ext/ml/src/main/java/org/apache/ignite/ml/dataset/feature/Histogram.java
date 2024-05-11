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
     * @return Bucket ids.
     */
    public Set<Integer> buckets();

    /**
     *
     * @param bucketId Bucket id.
     * @return Value in according to bucket id.
     */
    public Optional<Double> getValue(Integer bucketId);

    /**
     * @param other Other histogram.
     * @return Sum of this and other histogram.
     */
    public H plus(H other);

    /**
     * Compares histogram with other and returns true if they are equals
     *
     * @param other Other histogram.
     * @return True if histograms are equal.
     */
    public boolean isEqualTo(H other);
}
