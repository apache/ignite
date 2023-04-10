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
import lombok.Data;
import org.apache.ignite.internal.management.api.BaseCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.management.api.Parameter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.Scope;

/**
 *
 */
@Data
public class TracingConfigurationResetAllCommand extends BaseCommand implements ExperimentalCommand {
    /** */
    @Parameter(optional = true)
    private Scope scope;

    /** {@inheritDoc} */
    @Override public String description() {
        return "Reset all specific tracing configuration the to default. " +
            "If --scope is specified, then remove all label specific configuration " +
            "for the given scope and reset given scope specific configuration to the default, " +
            "if --scope is skipped then reset all tracing configurations to the default. " +
            "Print tracing configuration";
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeEnum(out, scope);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(protoVer, in);

        scope = U.readEnum(in, Scope.class);
    }
}
