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

package io.opencensus.exporter.trace;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.errorprone.annotations.MustBeClosed;
import io.opencensus.common.Duration;
import io.opencensus.common.Scope;
import io.opencensus.trace.Sampler;
import io.opencensus.trace.Span;
import io.opencensus.trace.Status;
import io.opencensus.trace.TraceOptions;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanExporter;
import io.opencensus.trace.export.SpanExporter.Handler;
import io.opencensus.trace.samplers.Samplers;

/**
 * Copy-pasted from OpenCensus library handler to avoid package-private access.
 *
 * An abstract class that allows different tracing services to export recorded data for sampled
 * spans in their own format within a given time frame. If export does not complete within the time
 * frame, spans will be dropped and no retries will be performed.
 *
 * <p>Only extend this class if the client APIs don't support timeout natively. If there is a
 * timeout option in the client APIs (for example Stackdriver Trace V2 API allows you to set
 * timeout), use that instead.
 *
 * <p>To export data this MUST be register to to the ExportComponent using {@link
 * SpanExporter#registerHandler(String, Handler)}.
 *
 * @since 0.22
 */
public abstract class TimeLimitedHandler extends SpanExporter.Handler {
    /** Logger. */
    private static final Logger logger = Logger.getLogger(TimeLimitedHandler.class.getName());

    /** Low probability sampler. */
    private static final Sampler lowProbabilitySampler = Samplers.probabilitySampler(0.0001);

    /** Tracer. */
    private final Tracer tracer;

    /** Deadline. */
    private final Duration deadline;

    /** Export span name. */
    private final String exportSpanName;

    /**
     * @param tracer Tracer.
     * @param deadline Deadline.
     * @param exportSpanName Export span name.
     */
    protected TimeLimitedHandler(Tracer tracer, Duration deadline, String exportSpanName) {
        this.tracer = tracer;
        this.deadline = deadline;
        this.exportSpanName = exportSpanName;
    }

    /**
     * Exports a list of sampled (see {@link TraceOptions#isSampled()}) {@link Span}s using the
     * immutable representation {@link SpanData}, within the given {@code deadline} of this {@link
     * TimeLimitedHandler}.
     *
     * @param spanDataList a list of {@code SpanData} objects to be exported.
     * @throws Exception throws exception when failed to export.
     * @since 0.22
     */
    public abstract void timeLimitedExport(Collection<SpanData> spanDataList) throws Exception;

    /** {@inheritDoc} */
    @Override public void export(final Collection<SpanData> spanDataList) {
        final Scope exportScope = newExportScope();
        try {
            TimeLimiter timeLimiter = SimpleTimeLimiter.create(Executors.newSingleThreadExecutor());
            timeLimiter.callWithTimeout(
                new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        timeLimitedExport(spanDataList);
                        return null;
                    }
                },
                deadline.toMillis(),
                TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            handleException(e, "Timeout when exporting traces: " + e);
        }
        catch (InterruptedException e) {
            handleException(e, "Interrupted when exporting traces: " + e);
        }
        catch (Exception e) {
            handleException(e, "Failed to export traces: " + e);
        }
        finally {
            exportScope.close();
        }
    }

    /**
     *
     */
    @MustBeClosed
    private Scope newExportScope() {
        return tracer.spanBuilder(exportSpanName).setSampler(lowProbabilitySampler).startScopedSpan();
    }

    /**
     * @param e Exception.
     * @param logMessage Logger message.
     */
    private void handleException(Exception e, String logMessage) {
        Status status = e instanceof TimeoutException ? Status.DEADLINE_EXCEEDED : Status.UNKNOWN;
        tracer
            .getCurrentSpan()
            .setStatus(
                status.withDescription(
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        logger.log(Level.WARNING, logMessage);
    }
}
