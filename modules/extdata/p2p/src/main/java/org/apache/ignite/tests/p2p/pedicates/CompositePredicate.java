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

package org.apache.ignite.tests.p2p.pedicates;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.binary.BinaryObject;

/** */
public class CompositePredicate<K> extends BinaryPredicate<K> implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 238742479L;

    /** Predicates. */
    private final Collection<BinaryPredicate> predicates = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public boolean apply(BinaryObject bo) {
        log.info("CompositePredicate on " + ignite.configuration().getIgniteInstanceName());

        for (BinaryPredicate predicate : predicates) {
            predicate.ignite = ignite;
            predicate.log = log;

            if (!predicate.apply(bo))
                return false;
        }

        return true;
    }

    /**
     * @param predicate Binary predicate.
     */
    public void addPredicate(BinaryPredicate predicate) {
        predicates.add(predicate);
    }
}
