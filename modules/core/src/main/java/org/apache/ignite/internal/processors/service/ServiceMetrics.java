package org.apache.ignite.internal.processors.service;

import java.lang.reflect.Method;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** TODO : comment */
public class ServiceMetrics {
    /** TODO : comment */
    public static final String METRIC_INVOCATION_HIST = "invocations";

    /** TODO : comment */
    public static final String SERVICE_METRICS = metricName("services", "byName");

    /** */
    private static final String HIST_INVOCATION_DESCR = "Histogram of service method durations in milliseconds.";

    /** */
    private static final long[] DEFAULT_INVOCATION_BOUNDS = new long[] {1, 5, 10, 50, 100, 200, 500, 1000, 5000};

    /** */
    private final GridMetricManager metricMgr;

    /** TODO : comment */
    public ServiceMetrics(GridMetricManager metricMgr) {
        this.metricMgr = metricMgr;
    }

    /** TODO : description : creates metric */
    public HistogramMetricImpl serviceInvocationsHist(String srvcName, Method method) {
        MetricRegistry metricRegistry = metricMgr.registry(metricName(SERVICE_METRICS, srvcName,
            methodMetricName(method)));

        if (metricRegistry == null)
            return null;

        return metricRegistry.histogram( METRIC_INVOCATION_HIST, DEFAULT_INVOCATION_BOUNDS, HIST_INVOCATION_DESCR );
    }

    /** TODO : comment */
    private String methodMetricName(Method method) {
        StringBuilder sb = new StringBuilder();

        sb.append(method.getName());
        sb.append("(");
        sb.append(Stream.of(method.getParameterTypes()).map(Class::getSimpleName)
            .collect(Collectors.joining(", ")));
        sb.append(")");

        return sb.toString();
    }

    /** TODO : implment */
    public void registerMetrics(Service srvc, ServiceContext srvcCtx) {
        MetricRegistry metricRegistry = metricMgr.registry(metricName(SERVICE_METRICS, srvcCtx.name()));

        for (Class<?> iface : srvc.getClass().getInterfaces()) {
            for (Method m : iface.getMethods()) {
                String metricName = methodMetricName(m);
            }
        }
    }

    /** TODO : implment */
    public void unregisterMetrics(Service srvc) {

    }
}
