// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.metrics.jdk;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.metrics.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.lang.management.*;
import java.net.*;
import java.util.*;

/**
 * This class provides JDK MXBean based local VM metrics. Note that average
 * CPU load cannot be obtained from JDK 1.5 and on some operating systems
 * (including Windows Vista) even from JDK 1.6 (although JDK 1.6 supposedly added
 * support for it).
 * <p>
 * If CPU load cannot be obtained from JDK, then {@link GridLocalMetrics#getCurrentCpuLoad()}
 * method will always return {@code -1}.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * The following configuration parameters are optional:
 * <ul>
 * <li>CPU update period (see {@link #setCpuLoadUpdateFrequency(long)})</li>
 * <li>File system root (see {@link #setFileSystemRoot(String)})</li>
 * <li>File system metrics enabled (see {@link #setFileSystemMetricsEnabled(boolean)})</li>
 * </ul>
 *
 * @author @java.author
 * @version @java.version
 */
@GridSpiInfo(
    author = /*@java.spi.author*/"GridGain Systems",
    url = /*@java.spi.url*/"www.gridgain.com",
    email = /*@java.spi.email*/"support@gridgain.com",
    version = /*@java.spi.version*/"x.x")
@GridSpiMultipleInstancesSupport(true)
public class GridJdkLocalMetricsSpi extends GridSpiAdapter implements GridLocalMetricsSpi,
    GridJdkLocalMetricsSpiMBean {
    /** Default update frequency. */
    public static final long DFLT_UPDATE_FREQ = 1000;

    /** */
    private MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

    /** */
    private OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    /** */
    private RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

    /** */
    private ThreadMXBean threads = ManagementFactory.getThreadMXBean();

    /** */
    private Collection<GarbageCollectorMXBean> gc = ManagementFactory.getGarbageCollectorMXBeans();

    /** Grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** */
    private GridLocalMetrics metrics;

    /** */
    private String fsRoot;

    /** */
    private File fsRootFile;

    /** */
    private boolean enableFsMetrics;

    /** */
    private GridWorker updater;

    /** */
    private long updateFreq = DFLT_UPDATE_FREQ;

    /** */
    private volatile double gcCpuLoad;

    /** */
    private volatile double cpuLoad;

    /**
     * Sets CPU metrics update frequency in milliseconds. CPU load and GC CPU load metrics
     * will return average load on this provided period of time.
     * <p>
     * If not provided, default value is {@link #DFLT_UPDATE_FREQ}.
     *
     * @param updateFreq CPU load metrics update period in milliseconds. Must be above {@code 0}.
     */
    @GridSpiConfiguration(optional = true)
    public void setCpuLoadUpdateFrequency(long updateFreq) {
        A.ensure(updateFreq > 0, "updateFreq > 0");

        this.updateFreq = updateFreq;
    }

    /** {@inheritDoc} */
    @Override public long getCpuLoadUpdateFrequency() {
        return updateFreq;
    }

    /**
     * Set file system root.
     *
     * @param fsRoot File system root.
     */
    @GridSpiConfiguration(optional = true)
    public void setFileSystemRoot(String fsRoot) {
        this.fsRoot = fsRoot;
    }

    /** {@inheritDoc} */
    @Override public String getFileSystemRoot() {
        return fsRoot;
    }

    /**
     * Enables or disables file system metrics (default is {@code false}). These metrics may be
     * expensive to get in certain environments and are disabled by default. Following metrics
     * are part of file system metrics:
     * <ul>
     * <li>{@link GridLocalMetrics#getFileSystemFreeSpace()}</li>
     * <li>{@link GridLocalMetrics#getFileSystemTotalSpace()}</li>
     * <li>{@link GridLocalMetrics#getFileSystemUsableSpace()}</li>
     * </ul>
     *
     * @param enableFsMetrics Flag indicating whether file system metrics are enabled.
     */
    @GridSpiConfiguration(optional = true)
    public void setFileSystemMetricsEnabled(boolean enableFsMetrics) {
        this.enableFsMetrics = enableFsMetrics;
    }

    /** {@inheritDoc} */
    @Override public boolean isFileSystemMetricsEnabled() {
        return enableFsMetrics;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
        startStopwatch();

        updater = new GridWorker(gridName, "gridgain-metrics-updater", log) {
            private long prevGcTime = -1;
            private long prevCpuTime = -1;

            @Override protected void body() throws GridInterruptedException {
                while (!isCancelled()) {
                    U.sleep(updateFreq);

                    gcCpuLoad = getGcCpuLoad();
                    cpuLoad = getCpuLoad();
                }
            }

            private double getGcCpuLoad() {
                long gcTime = 0;

                for (GarbageCollectorMXBean bean : gc) {
                    long colTime = bean.getCollectionTime();

                    if (colTime > 0)
                        gcTime += colTime;
                }

                gcTime /= getAvailableProcessors();

                double gc = 0;

                if (prevGcTime > 0) {
                    long gcTimeDiff = gcTime - prevGcTime;

                    gc = (double)gcTimeDiff / updateFreq;
                }

                prevGcTime = gcTime;

                return gc;
            }

            private double getCpuLoad() {
                long cpuTime;

                try {
                     cpuTime = U.<Long>property(os, "processCpuTime");
                }
                catch (GridRuntimeException ignored) {
                    return -1;
                }

                // Method reports time in nanoseconds across all processors.
                cpuTime /= 1000000 * getAvailableProcessors();

                double cpu = 0;

                if (prevCpuTime > 0) {
                    long cpuTimeDiff = cpuTime - prevCpuTime;

                    // CPU load could go higher than 100% because calculating of cpuTimeDiff also takes some time.
                    cpu = Math.min(1.0, (double)cpuTimeDiff / updateFreq);
                }

                prevCpuTime = cpuTime;

                return cpu;
            }
        };

        new GridThread(updater).start();

        if (enableFsMetrics) {
            if (fsRoot != null) {
                fsRootFile = new File(fsRoot);

                if (!fsRootFile.exists()) {
                    U.warn(log, "Invalid file system root name: " + fsRoot);

                    fsRootFile = null;
                }
            }
            else
                fsRootFile = getDefaultFileSystemRoot();
        }
        else
            fsRootFile = null;

        registerMBean(gridName, this, GridJdkLocalMetricsSpiMBean.class);

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> getNodeAttributes() throws GridSpiException {
        long totSysMemory = -1;

        try {
            totSysMemory = U.<Long>property(os, "totalPhysicalMemorySize");
        }
        catch (RuntimeException ignored) {
            // No-op.
        }

        return F.<String, Object>asMap(GridNodeAttributes.ATTR_PHY_RAM, totSysMemory);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws GridSpiException {
        U.cancel(updater);
        U.join(updater, log);

        unregisterMBean();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public GridLocalMetrics getMetrics() {
        if (metrics == null) {
            metrics = new GridLocalMetrics() {
                @Override public int getAvailableProcessors() {
                    return os.getAvailableProcessors();
                }

                @Override public double getCurrentCpuLoad() {
                    return cpuLoad;
                }

                @Override public double getCurrentGcCpuLoad() {
                    return gcCpuLoad;
                }

                @Override public long getHeapMemoryInitialized() {
                    return mem.getHeapMemoryUsage().getInit();
                }

                @Override public long getHeapMemoryUsed() {
                    return mem.getHeapMemoryUsage().getUsed();
                }

                @Override public long getHeapMemoryCommitted() {
                    return mem.getHeapMemoryUsage().getCommitted();
                }

                @Override public long getHeapMemoryMaximum() {
                    return mem.getHeapMemoryUsage().getMax();
                }

                @Override public long getNonHeapMemoryInitialized() {
                    return nonHeapMemoryUsage().getInit();
                }

                @Override public long getNonHeapMemoryUsed() {
                    return nonHeapMemoryUsage().getUsed();
                }

                @Override public long getNonHeapMemoryCommitted() {
                    return nonHeapMemoryUsage().getCommitted();
                }

                @Override public long getNonHeapMemoryMaximum() {
                    return nonHeapMemoryUsage().getMax();
                }

                @Override public long getUptime() {
                    return rt.getUptime();
                }

                @Override public long getStartTime() {
                    return rt.getStartTime();
                }

                @Override public int getThreadCount() {
                    return threads.getThreadCount();
                }

                @Override public int getPeakThreadCount() {
                    return threads.getPeakThreadCount();
                }

                @Override public long getTotalStartedThreadCount() {
                    return threads.getTotalStartedThreadCount();
                }

                @Override public int getDaemonThreadCount() {
                    return threads.getDaemonThreadCount();
                }

                @Override public long getFileSystemFreeSpace() {
                    return fsRootFile == null ? -1 : fsRootFile.getFreeSpace();
                }

                @Override public long getFileSystemTotalSpace() {
                    return fsRootFile == null ? -1 : fsRootFile.getTotalSpace();
                }

                @Override public long getFileSystemUsableSpace() {
                    return fsRootFile == null ? -1 : fsRootFile.getUsableSpace();
                }
            };
        }

        return metrics;
    }

    /**
     * Returns file system root where GridGain JAR was located.
     *
     * @return File system root.
     */
    @Nullable private File getDefaultFileSystemRoot() {
        URL clsUrl = getClass().getResource(getClass().getSimpleName() + ".class");

        try {
            String path = null;

            if (clsUrl != null)
                path = "jar".equals(clsUrl.getProtocol()) ? new URL(clsUrl.getPath()).getPath() : clsUrl.getPath();

            if (path != null) {
                File dir = new File(path);

                dir = dir.getParentFile();

                while (dir.getParentFile() != null)
                    dir = dir.getParentFile();

                return dir;
            }
        }
        catch (MalformedURLException ignored) {
            // No-op.
        }

        return null;
    }

    /**
     * @return Memory usage of non-heap memory.
     */
    private MemoryUsage nonHeapMemoryUsage() {
        // Workaround of exception in WebSphere.
        // We received the following exception:
        // java.lang.IllegalArgumentException: used value cannot be larger than the committed value
        // at java.lang.management.MemoryUsage.<init>(MemoryUsage.java:105)
        // at com.ibm.lang.management.MemoryMXBeanImpl.getNonHeapMemoryUsageImpl(Native Method)
        // at com.ibm.lang.management.MemoryMXBeanImpl.getNonHeapMemoryUsage(MemoryMXBeanImpl.java:143)
        // at org.gridgain.grid.spi.metrics.jdk.GridJdkLocalMetricsSpi.getMetrics(GridJdkLocalMetricsSpi.java:242)
        //
        // We so had to workaround this with exception handling, because we can not control classes from WebSphere.
        try {
            return mem.getNonHeapMemoryUsage();
        }
        catch (IllegalArgumentException ignored) {
            return new MemoryUsage(0, 0, 0, 0);
        }
    }

    /** {@inheritDoc} */
    @Override public int getAvailableProcessors() {
        return getMetrics().getAvailableProcessors();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentCpuLoad() {
        return getMetrics().getCurrentCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public double getCurrentGcCpuLoad() {
        return getMetrics().getCurrentGcCpuLoad();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryInitialized() {
        return getMetrics().getHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryUsed() {
        return getMetrics().getHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryCommitted() {
        return getMetrics().getHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getHeapMemoryMaximum() {
        return getMetrics().getHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryInitialized() {
        return getMetrics().getNonHeapMemoryInitialized();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryUsed() {
        return getMetrics().getNonHeapMemoryUsed();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryCommitted() {
        return getMetrics().getNonHeapMemoryCommitted();
    }

    /** {@inheritDoc} */
    @Override public long getNonHeapMemoryMaximum() {
        return getMetrics().getNonHeapMemoryMaximum();
    }

    /** {@inheritDoc} */
    @Override public long getUptime() {
        return getMetrics().getUptime();
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return getMetrics().getStartTime();
    }

    /** {@inheritDoc} */
    @Override public int getThreadCount() {
        return getMetrics().getThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getPeakThreadCount() {
        return getMetrics().getPeakThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getTotalStartedThreadCount() {
        return getMetrics().getTotalStartedThreadCount();
    }

    /** {@inheritDoc} */
    @Override public int getDaemonThreadCount() {
        return getMetrics().getDaemonThreadCount();
    }

    /** {@inheritDoc} */
    @Override public long getFileSystemFreeSpace() {
        return getMetrics().getFileSystemFreeSpace();
    }

    /** {@inheritDoc} */
    @Override public long getFileSystemTotalSpace() {
        return getMetrics().getFileSystemTotalSpace();
    }

    /** {@inheritDoc} */
    @Override public long getFileSystemUsableSpace() {
        return getMetrics().getFileSystemUsableSpace();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJdkLocalMetricsSpi.class, this);
    }
}
