/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests.p2p.pedicates;

import org.apache.ignite.binary.BinaryObject;

/** */
public class SecondConsideredPredicate extends BinaryPredicate {
    /** Serial version uid. */
    private static final long serialVersionUID = 238742456L;

    /** {@inheritDoc} */
    @Override public boolean apply(BinaryObject bo) {
        log.info("SecondConsideredPredicate on " + ignite.configuration().getIgniteInstanceName());

        return !bo.hasField("isDeleted");
    }
}
