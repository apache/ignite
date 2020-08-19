package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.junit.jupiter.api.Test;

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;

public class Tracing {

    @Test
    void config() {
        //tag::config[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setTracingSpi(new org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi());

        Ignite ignite = Ignition.start(cfg);
        //end::config[]

        ignite.close();
    }

    void enableSampling() {
        //tag::enable-sampling[]
        Ignite ignite = Ignition.start();

        ignite.tracingConfiguration().set(
                new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
                new TracingConfigurationParameters.Builder().withSamplingRate(0.5).build());

        //end::enable-sampling[]
    }

    @Test
    void exportToZipkin() {
        //tag::export-to-zipkin[]
        ZipkinTraceExporter.createAndRegister(
                ZipkinExporterConfiguration.builder().setV2Url("http://localhost:9411/api/v2/spans")
                        .setServiceName("ignite-cluster").build());

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setTracingSpi(new org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi());

        Ignite ignite = Ignition.start(cfg);
        //end::config[]
        //end::export-to-zipkin[]
        ignite.close();
    }
}
