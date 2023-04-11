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

package org.apache.ignite.internal.management.tracing;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.BaseCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.Scope;

/**
 *
 */
public class TracingConfigurationResetCommand extends BaseCommand implements ExperimentalCommand {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true)
    private Scope scope;

    /** */
    @Argument(optional = true)
    private boolean label;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Reset specific tracing configuration to the default. " +
            "If both --scope and --label are specified then remove given configuration, " +
            "if only --scope is specified then reset given configuration to the default. " +
            "Print reseted configuration";
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeEnum(out, scope);
        out.writeBoolean(label);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        scope = U.readEnum(in, Scope.class);
        label = in.readBoolean();
    }
}
