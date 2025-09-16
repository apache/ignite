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
import org.apache.ignite.internal.management.api.EnumDescription;
import org.apache.ignite.internal.management.tracing.TracingConfigurationCommand.TracingConfigurationCommandArg;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.Scope;

/** */
public class TracingConfigurationGetAllCommandArg extends TracingConfigurationCommandArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true, description = "Tracing span scope")
    @EnumDescription(
        names = {
            "DISCOVERY",
            "EXCHANGE",
            "COMMUNICATION",
            "TX",
            "SQL"
        },
        descriptions = {
            "Discovery scope",
            "Exchange scope",
            "Communication scope",
            "Transactional scope",
            "SQL scope"
        }
    )
    private Scope scope;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        U.writeEnum(out, scope);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(in);

        scope = U.readEnum(in, Scope.class);
    }

    /** */
    public Scope scope() {
        return scope;
    }

    /** */
    public void scope(Scope scope) {
        this.scope = scope;
    }
}
