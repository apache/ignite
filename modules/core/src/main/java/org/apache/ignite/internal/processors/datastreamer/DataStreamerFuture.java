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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data streamer future.
 */
class DataStreamerFuture extends GridFutureAdapter<Object> {
    /** Data loader. */
    @GridToStringExclude
    private DataStreamerImpl dataLdr;

    /**
     * @param dataLdr Data streamer.
     */
    DataStreamerFuture(DataStreamerImpl dataLdr) {
        assert dataLdr != null;

        this.dataLdr = dataLdr;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        if (onCancelled()) {
            dataLdr.closeEx(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStreamerFuture.class, this, super.toString());
    }
}