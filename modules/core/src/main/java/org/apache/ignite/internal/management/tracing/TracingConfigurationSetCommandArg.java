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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.Scope;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/** */
public class TracingConfigurationSetCommandArg extends TracingConfigurationGetCommandArg {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Argument(optional = true, example = "Decimal value between 0 and 1, where 0 means never and 1 means always. " +
        "More or less reflects the probability of sampling specific trace.")
    private double samplingRate;

    /** */
    @Argument(optional = true, example = "Set of scopes with comma as separator  DISCOVERY|EXCHANGE|COMMUNICATION|TX|SQL")
    private Scope[] includedScopes;

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        super.writeExternalData(out);

        out.writeDouble(samplingRate);
        U.writeArray(out, includedScopes);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternalData(in);

        samplingRate = in.readDouble();
        includedScopes = U.readArray(in, Scope.class);
    }

    /** */
    public double samplingRate() {
        return samplingRate;
    }

    /** */
    public void samplingRate(double samplingRate) {
        if (samplingRate < SAMPLING_RATE_NEVER || samplingRate > SAMPLING_RATE_ALWAYS)
            throw new IllegalArgumentException(
                "Invalid sampling-rate '" + samplingRate + "'. Decimal value between 0 and 1 should be used.");

        this.samplingRate = samplingRate;
    }

    /** */
    public Scope[] includedScopes() {
        return includedScopes;
    }

    /** */
    public void includedScopes(Scope[] includedScopes) {
        this.includedScopes = includedScopes;
    }
}
