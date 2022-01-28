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

package org.apache.ignite.spi.tracing.opencensus;

import io.opencensus.trace.Tracing;
import io.opencensus.trace.export.SpanExporter;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * Wrapper of OpenCensus trace exporters adopted for Ignite lifecycle.
 */
public class OpenCensusTraceExporter {
    /** Span exporter handler. */
    private final SpanExporter.Handler hnd;

    /** Trace exporter handler name. */
    private String hndName;

    /**
     * @param hnd Span exporter handler.
     */
    public OpenCensusTraceExporter(SpanExporter.Handler hnd) {
        this.hnd = hnd;
    }

    /**
     * Starts trace exporter on given node with name {@code igniteInstanceName}.
     *
     * @param igniteInstanceName Name of ignite instance.
     */
    public void start(String igniteInstanceName) {
        try {
            hndName = hnd.getClass().getName() + "-" + igniteInstanceName;

            Tracing.getExportComponent().getSpanExporter().registerHandler(hndName, hnd);
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to start " + this, e);
        }
    }

    /**
     * Stops trace exporter.
     */
    public void stop() {
        Tracing.getExportComponent().getSpanExporter().unregisterHandler(hndName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "OpenCensus trace exporter [hndName=" + hndName + ", hnd=" + hnd + "]";
    }
}
