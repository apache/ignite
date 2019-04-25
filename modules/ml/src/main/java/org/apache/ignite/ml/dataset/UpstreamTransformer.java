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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import java.util.stream.Stream;

/**
 * Interface of transformer of upstream.
 */
// TODO: IGNITE-10297: Investigate possibility of API change.
@FunctionalInterface
public interface UpstreamTransformer extends Serializable {
    /**
     * Transform upstream.
     *
     * @param upstream Upstream to transform.
     * @return Transformed upstream.
     */
    public Stream<UpstreamEntry> transform(Stream<UpstreamEntry> upstream);
}
