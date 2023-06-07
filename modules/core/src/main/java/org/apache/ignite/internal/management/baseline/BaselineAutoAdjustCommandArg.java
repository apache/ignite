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

package org.apache.ignite.internal.management.baseline;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.CliConfirmArgument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.management.baseline.BaselineCommand.VisorBaselineTaskArg;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
@CliConfirmArgument
@ArgumentGroup(value = {"enabled", "timeout"}, optional = false)
public class BaselineAutoAdjustCommandArg extends VisorBaselineTaskArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Positional
    @Argument(optional = true)
    private Enabled enabled;

    /** */
    @Argument(optional = true, example = "<timeoutMillis>", withoutPrefix = true)
    private Long timeout;

    /** */
    public enum Enabled {
        /** */
        DISABLE,

        /** */
        ENABLE
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeEnum(out, enabled);
        out.writeObject(timeout);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        enabled = U.readEnum(in, Enabled.class);
        timeout = (Long)in.readObject();
    }

    /** */
    public Enabled enabled() {
        return enabled;
    }

    /** */
    public void enabled(Enabled enabled) {
        this.enabled = enabled;
    }

    /** */
    public Long timeout() {
        return timeout;
    }

    /** */
    public void timeout(Long timeout) {
        this.timeout = timeout;
    }
}
