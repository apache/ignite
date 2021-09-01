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

package org.apache.ignite.internal.visor.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/** */
public class VisorBaselineAutoAdjustSettings extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** "Enable" flag. */
    public Boolean enabled;

    /** Soft timeout. */
    public Long softTimeout;

    /** Default constructor. */
    public VisorBaselineAutoAdjustSettings() {
    }

    /** Constructor. */
    public VisorBaselineAutoAdjustSettings(Boolean enabled, Long softTimeout) {
        this.enabled = enabled;
        this.softTimeout = softTimeout;
    }

    /**
     * @return "Enable" flag.
     */
    public Boolean getEnabled() {
        return enabled;
    }

    /**
     * Soft timeout.
     */
    public Long getSoftTimeout() {
        return softTimeout;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled != null);

        if (enabled != null)
            out.writeBoolean(enabled);

        out.writeBoolean(softTimeout != null);

        if (softTimeout != null)
            out.writeLong(softTimeout);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            enabled = in.readBoolean();

        if (in.readBoolean())
            softTimeout = in.readLong();
    }
}
