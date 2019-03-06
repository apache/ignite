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

package org.apache.ignite.ml.tree.randomforest.data.impurity;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;

/**
 * Helper class for ImpurityHistograms.
 */
public abstract class ImpurityHistogram implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -8982240673834216798L;

    /** Bucket ids. */
    protected final Set<Integer> bucketIds = new TreeSet<>();

    /** Feature id. */
    protected final int featureId;

    /**
     * Creates an instance of ImpurityHistogram.
     *
     * @param featureId Feature id.
     */
    public ImpurityHistogram(int featureId) {
        this.featureId = featureId;
    }

    /**
     * Checks split value validity and return Optional-wrap of it.
     * In other case returns Optional.empty
     *
     * @param bestBucketId Best bucket id.
     * @param bestSplitVal Best split value.
     * @param bestImpurity Best impurity.
     * @return best split value.
     */
    protected Optional<NodeSplit> checkAndReturnSplitValue(int bestBucketId, double bestSplitVal, double bestImpurity) {
        if (isLastBucket(bestBucketId))
            return Optional.empty();
        else
            return Optional.of(new NodeSplit(featureId, bestSplitVal, bestImpurity));
    }

    /**
     * @param bestBucketId Best bucket id.
     * @return true if best found bucket is last within all bucketIds.
     */
    private boolean isLastBucket(int bestBucketId) {
        int minBucketId = Integer.MAX_VALUE;
        int maxBucketId = Integer.MIN_VALUE;
        for (Integer bucketId : bucketIds) {
            minBucketId = Math.min(minBucketId, bucketId);
            maxBucketId = Math.max(maxBucketId, bucketId);
        }

        return bestBucketId == maxBucketId;
    }
}
