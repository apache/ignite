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
import java.io.Serializable;

/** */
public class VisorBaselineAutoAdjustSettings implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** "Enable" flag. */
    public final boolean enabled;

    /** Soft timeout. */
    public final long softTimeout;

    /** Constructor. */
    public VisorBaselineAutoAdjustSettings(boolean enabled, long softTimeout) {
        this.enabled= enabled;
        this.softTimeout = softTimeout;
    }

    /** */
    public static void writeExternalData(ObjectOutput out,
        VisorBaselineAutoAdjustSettings baselineAutoAdjustSettings) throws IOException {
        if (baselineAutoAdjustSettings == null)
            out.writeBoolean(false);
        else {
            out.writeBoolean(true);

            out.writeBoolean(baselineAutoAdjustSettings.enabled);

            out.writeLong(baselineAutoAdjustSettings.softTimeout);
        }
    }

    /** */
    public static VisorBaselineAutoAdjustSettings readExternalData(ObjectInput in) throws IOException {
        boolean autoAdjustSettingsNotNull = in.readBoolean();

        if (autoAdjustSettingsNotNull) {
            boolean autoAdjustmentEnabled = in.readBoolean();

            long timeout = in.readLong();

            return new VisorBaselineAutoAdjustSettings(autoAdjustmentEnabled, timeout);
        }

        return null;
    }
}
