package org.apache.ignite.compatibility.upgrade;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.xml.transform.TransformerConfigurationException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.startup.cmdline.CommandLineStartup;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import static org.apache.ignite.compatibility.upgrade.DistributionProvider.resolveBaseDist;
import static org.apache.ignite.compatibility.upgrade.DistributionProvider.resolveTargetDist;

/** */
public abstract class IgniteUpgradeAbstractTest extends GridCommonAbstractTest {
    /** Base Ignite Work Directory. */
    public static final String WORK_DIR = System.getenv(IgniteSystemProperties.IGNITE_WORK_DIR);

    /** Base Ignite Logging Directory. */
    public static final String LOG_DIR = System.getenv(IgniteSystemProperties.IGNITE_LOG_DIR);

    /** Base Ignite Config Directory. */
    public static final String CONFIG_DIR = System.getProperty("IGNITE_CONFIG_DIR");

    /** Default Ignite Work Directory. */
    private static final String DEFAULT_WORK_DIR = System.getProperty("user.dir") + "/target/upgrade-test/work";

    /** Default Ignite Work Directory. */
    private static final String DEFAULT_LOG_DIR = System.getProperty("user.dir") + "/target/upgrade-test/logs";

    /** Default Ignite Config Directory. */
    private static final String DEFAULT_CONFIG_DIR = System.getProperty("user.dir") + "/target/upgrade-test/config";

    /** Proxies for Multi-JVM nodes. */
    private final ConcurrentMap<Integer, IgniteProcessProxy> gridProxies = new ConcurrentHashMap<>();

    /** Configuration generator. */
    private final IgniteConfigGenerator cfgGenerator = new IgniteConfigGenerator();

    /** Local client instance. */
    private IgniteEx locJvmInstance;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        deleteRootDir(WORK_DIR != null ? WORK_DIR : DEFAULT_WORK_DIR);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i : gridProxies.keySet())
            stopNode(i);

        gridProxies.clear();

        locJvmInstance.close();

        super.afterTest();
    }

    /** */
    protected IgniteEx startBaseCluster(int nodeCnt) throws Exception {
        IgniteEx ign = createOrRestartNode(1, true, false);

        for (int i = 2; i <= nodeCnt; i++)
            createOrRestartNode(i, true, false);

        return ign;
    }

    /** */
    protected IgniteEx startBaseNode(int idx) throws Exception {
        return createOrRestartNode(idx, true, false);
    }

    /** */
    protected IgniteEx startBaseClientNode(int idx) throws Exception {
        return createOrRestartNode(idx, true, true);
    }

    /** */
    protected IgniteEx startTargetNode(int idx) throws Exception {
        return createOrRestartNode(idx, false, false);
    }

    /** */
    protected IgniteEx startTargetClientNode(int idx) throws Exception {
        return createOrRestartNode(idx, false, true);
    }

    /** */
    protected IgniteEx upgradeNode(int idx) throws Exception {
        return createOrRestartNode(idx, false, false);
    }

    /** */
    protected IgniteEx downgradeNode(int idx) throws Exception {
        return createOrRestartNode(idx, true, false);
    }

    /** */
    protected void stopNode(int idx) throws Exception {
        IgniteProcessProxy proxy = gridProxies.remove(idx);

        if (proxy != null)
            IgniteProcessProxy.stop(proxy.configuration().getIgniteInstanceName(), true);
    }

    /**
     * Centralized logic for node lifecycle.
     */
    private IgniteEx createOrRestartNode(int idx, boolean isBase, boolean isClient) throws Exception {
        assert idx > 0 : "Remote node index must be greater than 0 (provided index: " + idx + ")";

        if (gridProxies.containsKey(idx))
            stopNode(idx);

        File dist = isBase ? resolveBaseDist() : resolveTargetDist();
        String workDir = resolveDir(WORK_DIR != null ? WORK_DIR : DEFAULT_WORK_DIR, idx);
        String logDir = resolveDir(LOG_DIR != null ? LOG_DIR : DEFAULT_LOG_DIR, idx);
        String cfgDir = resolveDir(CONFIG_DIR != null ? CONFIG_DIR : DEFAULT_CONFIG_DIR, idx);

        IgniteConfiguration cfg;

        if (gridProxies.containsKey(idx))
            cfg = gridProxies.get(idx).configuration();
        else {
            cfg = getConfiguration(getTestIgniteInstanceName(idx));
            cfg.setClientMode(isClient);
            cfg.setWorkDirectory(workDir);
        }

        cfg.setIgniteHome(dist.getAbsolutePath());

        Path xmlCfgPath = cfgGenerator.generateIgniteConfigurationXml(cfg, logDir, cfgDir);

        String label = isBase ? "[BASE]" : "[TRGT]";

        IgniteProcessProxy ign = new IgniteProcessProxy(cfg, log, locJvmInstance == null ? null : () -> locJvmInstance, false, Collections.emptyList()) {
            @Override protected IgniteLogger logger(IgniteLogger log, Object ctgr) {
                return log.getLogger(ctgr + "#" + label + "node-" + idx);
            }

            @Override protected String igniteNodeRunnerClassName() throws Exception {
                return CommandLineStartup.class.getCanonicalName();
            }

            @Override protected String params(IgniteConfiguration cfg, boolean resetDiscovery) throws Exception {
                return xmlCfgPath.toAbsolutePath().toString();
            }

            @Override protected Collection<String> filteredJvmArgs() throws Exception {
                Collection<String> filteredArgs = new ArrayList<>();

                for (String arg : super.filteredJvmArgs()) {
                    if (arg.startsWith("-cp") || arg.startsWith("-classpath"))
                        continue; // Skip the parent's CP

                    filteredArgs.add(arg);
                }

                String customCp = buildIgniteClasspath(dist);

                filteredArgs.add("-DIGNITE_HOME=" + dist.getAbsolutePath());

                filteredArgs.add("-cp");
                filteredArgs.add(customCp);

                return filteredArgs;
            }};

        if (locJvmInstance == null) {
            locJvmInstance = startClientGrid(getConfiguration(getTestIgniteInstanceName(0)));

            ign.localJvmGrid(() -> locJvmInstance);
        }

        gridProxies.put(idx, ign);

        return ign;
    }

    /**
     * Resolves a specific directory for a node index and ensures it exists.
     */
    private String resolveDir(String root, int idx) {
        File dir = new File(root, "node_" + idx);

        if (!dir.exists())
            dir.mkdirs();

        return dir.getAbsolutePath();
    }

    /**
     * Deletes the entire root directory and all subdirectories/files.
     */
    private void deleteRootDir(String root) throws IOException {
        Path rootPath = Paths.get(root);

        if (Files.exists(rootPath)) {
            try (Stream<Path> walk = Files.walk(rootPath)) {
                walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        }
    }

    /**
     * Builds a classpath string based on the provided Ignite home directory.
     *
     * @param igniteHome The root directory of the Ignite distribution.
     * @return A classpath string with OS-specific separators.
     */
    protected String buildIgniteClasspath(File igniteHome) {
        String sep = File.pathSeparator;
        String wildCard = File.separator + "*";
        StringBuilder cp = new StringBuilder();

        File libsDir = new File(igniteHome, "libs");

        if (!libsDir.exists() || !libsDir.isDirectory())
            throw new IllegalArgumentException("Invalid Ignite home: 'libs' directory not found at " +
                libsDir.getAbsolutePath());

        cp.append(libsDir.getAbsolutePath()).append(wildCard);

        File[] subFiles = libsDir.listFiles();
        String[] requiredOptionals = {"ignite-log4j2", "ignite-spring"};

        if (subFiles != null) {
            for (File sub : subFiles) {
                if (sub.isDirectory()) {
                    if (!"optional".equals(sub.getName()))
                        cp.append(sep).append(sub.getAbsolutePath()).append(wildCard);
                    else {
                        for (String opt : requiredOptionals) {
                            File optModule = new File(sub, opt);

                            if (optModule.exists())
                                cp.append(sep).append(optModule.getAbsolutePath()).append(wildCard);
                        }
                    }
                }
            }
        }

        return cp.toString();
    }
}
