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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

/** */
public class VisorDrNodeTaskArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;
    /** Config. */
    private boolean config;
    /** Metrics. */
    private boolean metrics;
    /** Clear store. */
    private boolean clearStore;

    /**
     * Default constructor.
     */
    public VisorDrNodeTaskArgs() {
        // No-op.
    }

    /** */
    public VisorDrNodeTaskArgs(boolean config, boolean metrics, boolean clearStore) {
        this.config = config;
        this.metrics = metrics;
        this.clearStore = clearStore;
    }

    /** */
    public boolean config() {
        return config;
    }

    /** */
    public boolean metrics() {
        return metrics;
    }

    /** */
    public boolean clearStore() {
        return clearStore;
    }


    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(config);
        out.writeBoolean(metrics);
        out.writeBoolean(clearStore);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        config = in.readBoolean();
        metrics = in.readBoolean();
        clearStore = in.readBoolean();
    }
}
