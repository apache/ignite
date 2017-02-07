package org.apache.ignite.testframework.junits.multijvm;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.IgniteTestResources;

/**
 * Base class implementing a remote node.
 */
public class IgniteNodeProxyBase {
    /** Property that specify alternative {@code JAVA_HOME}. */
    protected static final String TEST_MULTIJVM_JAVA_HOME = "test.multijvm.java.home";

    /** Jvm process with ignite instance. */
    protected final transient GridJavaProcess proc;

    /** Configuration. */
    protected final transient IgniteConfiguration cfg;

    /** Logger. */
    protected final transient IgniteLogger log;

    /** Grid id. */
    protected final UUID id = UUID.randomUUID();

    /** Grid proxies. */
    protected static final ConcurrentMap<String, IgniteNodeProxyBase> gridProxies = new ConcurrentHashMap<>();

    /** Local JVM grid. */
    protected final transient Ignite locJvmGrid;

    /**
     * Constructor.
     *
     * @param cfg The configuration.
     * @param log The log.
     */
    public IgniteNodeProxyBase(IgniteConfiguration cfg, IgniteLogger log, Ignite locIg,
            NodeProcessParameters params) throws Exception {
        assert cfg != null;

        this.cfg = cfg;
        this.log = log.getLogger("jvm-" + id.toString().substring(0, id.toString().indexOf('-')));

        assert locIg != null;

        this.locJvmGrid = locIg;

        final CountDownLatch rmtNodeStartedLatch = new CountDownLatch(1);

        locJvmGrid.events().localListen(new NodeStartedListener(id, rmtNodeStartedLatch), EventType.EVT_NODE_JOINED);

        this.proc = createProcess(params);

        assert rmtNodeStartedLatch.await(30, TimeUnit.SECONDS): "Remote node has not joined [id=" + id + ", name=" + cfg.getGridName() +']';

        IgniteNodeProxyBase prevVal = gridProxies.putIfAbsent(cfg.getGridName(), this);

        if (prevVal != null) {
            prevVal.remoteCompute().run(new StopGridTask(cfg.getGridName(), true));

            throw new IllegalStateException("There was found instance assotiated with " + cfg.getGridName() +
               ", instance= " + prevVal + ". New started node was stopped.");
        }
    }

    /**
     * @return {@link IgniteCompute} instance to communicate with remote node.
     */
    public IgniteCompute remoteCompute() {
        ClusterGroup grp = locJvmGrid.cluster().forNodeId(id);

        if (grp.nodes().isEmpty())
            throw new IllegalStateException("Could not found node with id=" + id + ".");

        return locJvmGrid.compute(grp);
    }

    /** {@inheritDoc} */
    public Collection<ClusterNode> nodes() {
        return remoteCompute().call(new IgniteClusterProcessProxy.NodesTask());
    }

    /**
     * @param gridName Grid name.
     * @return Instance by name or exception wiil be thrown.
     */
    public static IgniteNodeProxyBase ignite(String gridName) {
        IgniteNodeProxyBase res = gridProxies.get(gridName);

        if (res == null)
            throw new IgniteIllegalStateException("Grid instance was not properly started " +
                "or was already stopped: " + gridName + ". All known grid instances: " + gridProxies.keySet());

        return res;
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
     * @return Local JVM grid instance.
     */
    public final Ignite localJvmGrid() {
        return locJvmGrid;
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

        String cfgFileName = IgniteNodeRunner.storeToFile(cfg.setNodeId(id).setConsistentId(id));

        Collection<String> filteredJvmArgs = new ArrayList<>();

        if (F.isEmpty(params.getJvmArguments())) {
            // Calculate JVM parameters based on his process options:
            Marshaller marsh = cfg.getMarshaller();

            if (marsh != null)
                filteredJvmArgs.add("-D" + IgniteTestResources.MARSH_CLASS_NAME + "=" + marsh.getClass().getName());

            for (String arg : U.jvmArgs()) {
                if (arg.startsWith("-Xmx") || arg.startsWith("-Xms")
                    || arg.startsWith("-XX")
                    || (marsh != null && arg.startsWith("-D" + IgniteTestResources.MARSH_CLASS_NAME)))
                    filteredJvmArgs.add(arg);
            }
        }
        else
            filteredJvmArgs.addAll(params.getJvmArguments());

        return GridJavaProcess.exec(
            IgniteNodeRunner.class.getCanonicalName(),
            cfgFileName, // Params.
            this.log,
            // Optional closure to be called each time wrapped process prints line to system.out or system.err.
            new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    IgniteNodeProxyBase.this.log.info(logPrefix() + s);
                }
            },
            null,
            System.getProperty(TEST_MULTIJVM_JAVA_HOME),
            filteredJvmArgs, // JVM Args.
            System.getProperty("surefire.test.class.path"),
            params.isUniqueProcDir() ? createUniqueDir("process") : null,
            params.getProcEnv()
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

    /**
     */
    private static class NodeStartedListener extends IgnitePredicateX<Event> {
        /** Id. */
        private final UUID id;

        /** Remote node started latch. */
        private final CountDownLatch rmtNodeStartedLatch;

        /**
         * @param id Id.
         * @param rmtNodeStartedLatch Remote node started latch.
         */
        NodeStartedListener(UUID id, CountDownLatch rmtNodeStartedLatch) {
            this.id = id;
            this.rmtNodeStartedLatch = rmtNodeStartedLatch;
        }

        /** {@inheritDoc} */
        @Override public boolean applyx(Event e) {
            if (((DiscoveryEvent)e).eventNode().id().equals(id)) {
                rmtNodeStartedLatch.countDown();

                return false;
            }

            return true;
        }
    }

    /**
     *
     */
    protected static class StopGridTask implements IgniteRunnable {
        /** Grid name. */
        private final String gridName;

        /** Cancel. */
        private final boolean cancel;

        /**
         * @param gridName Grid name.
         * @param cancel Cancel.
         */
        public StopGridTask(String gridName, boolean cancel) {
            this.gridName = gridName;
            this.cancel = cancel;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            G.stop(gridName, cancel);
        }
    }
}
