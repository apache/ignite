package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
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

    @Test
    void exportToZipkin() {
        //tag::export-to-zipkin[]
        ZipkinTraceExporter.createAndRegister(ZipkinExporterConfiguration.builder()
                .setV2Url("http://localhost:9411/api/v2/spans").setServiceName("ignite-cluster").build());

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setTracingSpi(new org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi());

        Ignite ignite = Ignition.start(cfg);
        //end::config[]
        //end::export-to-zipkin[]
        ignite.close();

    }
}
