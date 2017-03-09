package org.apache.ignite.testframework.junits.multijvm2;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.multijvm.IgniteClusterProcessProxy;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.testframework.junits.multijvm.NodeProcessParameters;

/**
 * Most of the code is copied from {@link IgniteProcessProxy}.
 * This class intended to handle situation when this process is not an Ignite node.
 * Static in this class represents registry of remote running nodes.
 */
public class IgniteNodeProxy2 {
    /** Property that specify alternative {@code JAVA_HOME}. */
    protected static final String TEST_MULTIJVM_JAVA_HOME = "test.multijvm.java.home";

    /** Grid proxies. */
    protected static final ConcurrentMap<String, IgniteNodeProxy2> gridProxies = new ConcurrentHashMap<>();

    /** Names order (random access). */
    protected static final List<String> nameOrderList = new CopyOnWriteArrayList<>();

    /** Jvm process with ignite instance. */
    protected final transient GridJavaProcess proc;

    /** Configuration. */
    protected final transient IgniteConfiguration cfg;

    /** Logger. */
    protected final transient IgniteLogger log;

    /** Remote Ignite node id. */
    protected final UUID id = UUID.randomUUID();

    /**
     * Constructor.
     *
     * @param cfg The configuration.
     * @param logr The log.
     */
    public IgniteNodeProxy2(IgniteConfiguration cfg, IgniteLogger logr, NodeProcessParameters params) throws Exception {
        assert cfg != null;

        this.cfg = cfg;
        this.log = logr.getLogger("jvm-" + id.toString().substring(0, id.toString().indexOf('-')));

        this.proc = createProcess(params);

        final String name = cfg.getGridName();

        IgniteNodeProxy2 prevVal = gridProxies.putIfAbsent(name, this);

        if (prevVal != null) {
            prevVal.stop();

            throw new IllegalStateException("There was found instance assotiated with " + cfg.getGridName() +
                ", instance= " + prevVal + ". New started node was stopped.");
        }

        nameOrderList.add(name);
    }

    /**
     * @param lclClient Local client Ignite instance.
     * @param cn The cluster node.
     * @return The number of nodes as seen from the specified node.
     */
    private static int topologySizeFromNodeViewpoint(Ignite lclClient, ClusterNode cn) {
        UUID id = cn.id();

        ClusterGroup grp = lclClient.cluster().forNodeId(id);

        IgniteCompute compute = lclClient.compute(grp);

        Collection<ClusterNode> nodes = compute.call(
            new IgniteClusterProcessProxy.ClusterTaskAdapter<Collection<ClusterNode>>() {
                /** {@inheritDoc} */
                @Override public Collection<ClusterNode> call() throws Exception {
                    return cluster().nodes();
                }
            });

        return nodes.size();
    }

    /**
     * @param requiredTopSize The desired topology size.
     * @return {@code true} if all nodes see exactly the required topology size.
     */
    public static boolean ensureTopology(final int requiredTopSize, IgniteConfiguration cfg) {
        Ignition.setClientMode(true);

        try (Ignite tmpClientIgnite = Ignition.start(cfg)) {
            // First, check it from the client node viewpoint:
            int act = tmpClientIgnite.cluster().nodes().size() - 1;

            if (requiredTopSize != act)
                return false;

            // Second, check topology from each node viewpoint:
            for (ClusterNode cn: tmpClientIgnite.cluster().nodes()) {
                act = -1 + topologySizeFromNodeViewpoint(tmpClientIgnite, cn);

                if (requiredTopSize != act)
                    return false;
            }
        }
        finally {
            Ignition.setClientMode(false);
        }

        return true;
    }

    /**
     *
     */
    public void stop() throws Exception {
        assert proc != null;

        try {
            proc.kill();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        boolean rm = gridProxies.remove(name(), this);

        assert rm;
    }

    /**
     *
     * @throws Exception
     */
    public static void stopAll() throws Exception {
        for (IgniteNodeProxy2 p: gridProxies.values())
            p.stop();
    }

    /**
     * @param gridName Grid name.
     * @return Instance by name or exception wiil be thrown.
     */
    public static IgniteNodeProxy2 node(String gridName) {
        IgniteNodeProxy2 res = gridProxies.get(gridName);

        if (res == null)
            throw new IgniteIllegalStateException("Grid instance was not properly started " +
                "or was already stopped: " + gridName + ". All known grid instances: " + gridProxies.keySet());

        return res;
    }

    /**
     * @param idx The index.
     * @return The node name by given index.
     */
    @SuppressWarnings("unused")
    public static String nodeName(int idx) {
        return nameOrderList.get(idx);
    }

    /**
     * @param idx The index.
     * @return The node.
     */
    public static IgniteNodeProxy2 node(int idx) {
        return node(nameOrderList.get(idx));
    }

    /**
     * Convenience getter to get the proxied grid name.
     *
     * @return The name.
     */
    public final String name() {
        return cfg.getGridName();
    }

    /**
     * @return Grid id.
     */
    public final UUID getId() {
        return id;
    }

    /**
     * Creates the node process.
     *
     * @return The process instance.
     * @throws Exception On error.
     */
    protected GridJavaProcess createProcess(NodeProcessParameters params) throws Exception {
        assert cfg.getGridName() != null : "name";

        cfg.setWorkDirectory(params.isUniqueWorkDir() ? createUniqueDir("work").getAbsolutePath() : null);

        @SuppressWarnings("deprecation")
        String cfgFileName = IgniteNodeRunner.storeToFile(cfg.setNodeId(id).setConsistentId(id), true);

        Collection<String> filteredJvmArgs = new ArrayList<>();

        if (F.isEmpty(params.getJvmArguments())) {
            // Inherit most of the parameters from this JVM parameters based on his process options:
            boolean marshAdded = false;

            for (String arg : U.jvmArgs()) {
                if (arg.startsWith("-Xmx") || arg.startsWith("-Xms") || arg.startsWith("-XX")
                    || arg.equals("-ea"))
                    filteredJvmArgs.add(arg);

                if (!marshAdded && arg.startsWith("-D" + IgniteTestResources.MARSH_CLASS_NAME)) {
                    filteredJvmArgs.add(arg);

                    marshAdded = true;
                }
            }

            if (!marshAdded) {
                Marshaller marsh = cfg.getMarshaller();

                if (marsh != null)
                    filteredJvmArgs.add("-D" + IgniteTestResources.MARSH_CLASS_NAME + "=" + marsh.getClass().getName());
            }
        }
        else
            // Use only the explicitly specified arguments:
            filteredJvmArgs.addAll(params.getJvmArguments());

        final String hhKey = "HADOOP_HOME";

        // Special treatment of HADOOP_HOME:
        String hhVal = IgniteSystemProperties.getString(hhKey);

        if (hhVal == null)
            throw new IgniteIllegalStateException("Please set " + hhKey);

        filteredJvmArgs.add("-D" + hhKey + "=" + hhVal);

        X.println("Args: " +filteredJvmArgs);

        return GridJavaProcess.exec(
            IgniteNodeRunner.class.getCanonicalName(),
            cfgFileName, // Params.
            this.log,
            new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    IgniteNodeProxy2.this.log.info(logPrefix() + " Out: " + s);
                }
            },
            new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    IgniteNodeProxy2.this.log.info(logPrefix() + " Err: " + s);
                }
            },
            null,
            System.getProperty(TEST_MULTIJVM_JAVA_HOME),
            filteredJvmArgs, // JVM Args.
            System.getProperty("surefire.test.class.path"),
            params.isUniqueProcDir() ? createUniqueDir("process") : null,
            params.getProcEnv(),
            true /* redirectStdErr */
        );
    }

    /**
     * Creates unique folder based on this UUID.
     *
     * @return Precreated unique dir, ready for use.
     */
    protected File createUniqueDir(String suffix) throws IgniteCheckedException {
        String tmpDirPath = System.getProperty("java.io.tmpdir");

        if (tmpDirPath == null)
            throw new IgniteCheckedException("Failed to create work directory in OS temp " +
                "(property 'java.io.tmpdir' is null).");

        File dir = new File(tmpDirPath, "ignite" + File.separator + suffix + "-" + name() + "-" + id);

        if (!dir.isAbsolute())
            throw new IgniteCheckedException("Work directory path must be absolute: " + dir);

        if (!U.mkdirs(dir))
            throw new IgniteCheckedException("Work directory does not exist and cannot be created: " + dir);

        if (!dir.canRead())
            throw new IgniteCheckedException("Cannot read from work directory: " + dir);

        if (!dir.canWrite())
            throw new IgniteCheckedException("Cannot write to work directory: " + dir);

        return dir;
    }

    /**
     * @return Jvm process in which grid node started.
     */
    public final GridJavaProcess getProcess() {
        return proc;
    }

    /**
     * Gets log prefix for the forked process output.
     *
     * @return The log prefix.
     */
    protected String logPrefix() {
        GridJavaProcess p = proc;

        if (p == null)
            return "";

        return "PID-" + p.getPid() + " ";
    }
}