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

package org.apache.ignite.internal.management.rollingupgrade;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;

public class RollingUpgradeTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean enabled;

    /** */
    private IgnitePair<IgniteProductVersion> rollUpVers;

    /** */
    private Throwable exception;

    /** */
    public RollingUpgradeTaskResult(boolean enabled, IgnitePair<IgniteProductVersion> rollUpVers, Throwable exception) {
        this.enabled = enabled;
        this.exception = exception;
        this.rollUpVers = rollUpVers;
    }

    /** */
    public RollingUpgradeTaskResult() {
        // No-op.
    }

    public boolean enabled() {
        return enabled;
    }

    public Throwable exception() {
        return exception;
    }

    public IgnitePair<IgniteProductVersion> rollUpVers() {
        return rollUpVers;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeObject(rollUpVers);
        out.writeObject(exception);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        this.enabled = in.readBoolean();
        this.rollUpVers = (IgnitePair<IgniteProductVersion>)in.readObject();
        this.exception = (Throwable)in.readObject();
    }

    /** */
    public void enabled(boolean enabled) {
        this.enabled = enabled;
    }

    /** */
    public void rollUpVers(IgnitePair<IgniteProductVersion> rollUpVers) {
        this.rollUpVers = rollUpVers;
    }

    /** */
    public void exception(Throwable e) {
        this.exception = e;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RollingUpgradeTaskResult.class, this);
    }
}
