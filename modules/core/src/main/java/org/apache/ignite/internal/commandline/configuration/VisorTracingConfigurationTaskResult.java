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

package org.apache.ignite.internal.commandline.configuration;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;

/**
 * Result for {@link VisorTracingConfigurationTask}.
 */
public class VisorTracingConfigurationTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final Character RES_PRINTER_SEPARATOR = ',';

    /** Retrieved reseted or updated tracing configuration. */
    private List<VisorTracingConfigurationItem> tracingConfigurations = new ArrayList<>();

    /**
     * Default constructor.
     */
    public VisorTracingConfigurationTaskResult() {
        // No-op.
    }

    /**
     * Add coordinates and parameters pair to the result.
     *
     * @param coordinates {@link TracingConfigurationCoordinates} instance.
     * @param parameters {@link TracingConfigurationParameters} instance.
     */
    @SuppressWarnings("unchecked")
    public void add(TracingConfigurationCoordinates coordinates, TracingConfigurationParameters parameters) {
        tracingConfigurations.add(new VisorTracingConfigurationItem(
            coordinates.scope(),
            coordinates.label(),
            parameters.samplingRate(),
            parameters.includedScopes()
        ));
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, tracingConfigurations);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked") @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        tracingConfigurations = (List)U.readCollection(in);
    }

    /**
     * Fills printer {@link Consumer <String>} by string view of this class.
     */
    public void print(Consumer<String> printer) {
        printer.accept("Scope, Label, Sampling Rate, included scopes");

        Collections.sort(tracingConfigurations, Comparator.comparing(VisorTracingConfigurationItem::scope));

        for (VisorTracingConfigurationItem tracingConfiguration : tracingConfigurations) {
            printer.accept(
                tracingConfiguration.scope().name() + RES_PRINTER_SEPARATOR +
                    (tracingConfiguration.label() == null ? "" : tracingConfiguration.label()) + RES_PRINTER_SEPARATOR +
                    tracingConfiguration.samplingRate() + RES_PRINTER_SEPARATOR +
                    Arrays.toString(tracingConfiguration.includedScopes().toArray()));
        }
    }

    /**
     * @return Retrieved reseted or updated tracing configuration.
     */
    public List<VisorTracingConfigurationItem> tracingConfigurations() {
        return tracingConfigurations;
    }
}
