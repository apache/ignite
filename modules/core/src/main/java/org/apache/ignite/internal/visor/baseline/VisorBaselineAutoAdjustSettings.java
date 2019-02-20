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
    private boolean enabled;

    /** Soft timeout. */
    private long softTimeout;

    /** Default constructor. */
    public VisorBaselineAutoAdjustSettings() {
    }

    /** Constructor. */
    public VisorBaselineAutoAdjustSettings(boolean enabled, long softTimeout) {
        this.enabled= enabled;
        this.softTimeout = softTimeout;
    }

    /** "Enable" flag. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Soft timeout. */
    public long getSoftTimeout() {
        return softTimeout;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeLong(softTimeout);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        enabled = in.readBoolean();
        softTimeout = in.readLong();
    }
}
