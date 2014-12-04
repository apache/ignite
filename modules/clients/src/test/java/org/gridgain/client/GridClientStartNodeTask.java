/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.springframework.beans.factory.*;
import org.springframework.context.support.*;

import java.net.*;
import java.util.*;

import static org.apache.ignite.compute.ComputeJobResultPolicy.*;

/**
 * Start node task, applicable arguments:
 * <ul>
 *     <li>tcp</li>
 *     <li>http</li>
 *     <li>tcp+ssl</li>
 *     <li>http+ssl</li>
 * </ul>
 */
public class GridClientStartNodeTask extends GridTaskSingleJobSplitAdapter<String, Integer> {
    /**
     * Available node's configurations.
     */
    private static final Map<String, String> NODE_CFG = new HashMap<String, String>() {{
        put("tcp", "modules/clients/src/test/resources/spring-server-node.xml");
        put("http", "modules/clients/src/test/resources/spring-server-node.xml");
        put("tcp+ssl", "modules/clients/src/test/resources/spring-server-ssl-node.xml");
        put("http+ssl", "modules/clients/src/test/resources/spring-server-ssl-node.xml");
    }};

    /** */
    @IgniteLoggerResource
    private transient IgniteLogger log;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, String type) throws GridException {
        log.info(">>> Starting new grid node [currGridSize=" + gridSize + ", arg=" + type + "]");

        if (type == null)
            throw new IllegalArgumentException("Node type to start should be specified.");

        IgniteConfiguration cfg = getConfig(type);

        // Generate unique for this VM grid name.
        String gridName = cfg.getGridName() + " (" + UUID.randomUUID() + ")";

        // Update grid name (required to be unique).
        cfg.setGridName(gridName);

        // Start new node in current VM.
        Ignite g =  G.start(cfg);

        log.info(">>> Grid started [nodeId=" + g.cluster().localNode().id() + ", name='" + g.name() + "']");

        return true;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
        throws GridException {
        if (res.getException() != null)
            return FAILOVER;

        return WAIT;
    }

    /**
     * Load grid configuration for specified node type.
     *
     * @param type Node type to load configuration for.
     * @return Grid configuration for specified node type.
     */
    static IgniteConfiguration getConfig(String type) {
        String path = NODE_CFG.get(type);

        if (path == null)
            throw new IllegalArgumentException("Unsupported node type: " + type);

        URL url = U.resolveGridGainUrl(path);

        BeanFactory ctx = new FileSystemXmlApplicationContext(url.toString());

        return (IgniteConfiguration)ctx.getBean("grid.cfg");
    }

    /**
     * Example for start/stop node tasks.
     *
     * @param args Not used.
     */
    public static void main(String[] args) {
        String nodeType = "tcp+ssl";

        // Start initial node = 1
        try (Ignite g = G.start(NODE_CFG.get(nodeType))) {
            // Change topology.
            changeTopology(g, 4, 1, nodeType);
            changeTopology(g, 1, 4, nodeType);

            // Stop node by id = 0
            g.compute().execute(GridClientStopNodeTask.class, g.cluster().localNode().id().toString());

            // Wait for node stops.
            //U.sleep(1000);

            assert G.allGrids().isEmpty();
        }
        catch (GridException e) {
            System.err.println("Uncaught exception: " + e.getMessage());

            e.printStackTrace(System.err);
        }
    }

    /**
     * Change topology.
     *
     * @param parent Grid to execute tasks on.
     * @param add New nodes count.
     * @param rmv Remove nodes count.
     * @param type Type of nodes to manipulate.
     * @throws GridException On any exception.
     */
    private static void changeTopology(Ignite parent, int add, int rmv, String type) throws GridException {
        Collection<ComputeTaskFuture<?>> tasks = new ArrayList<>();

        IgniteCompute comp = parent.compute().enableAsync();

        // Start nodes in parallel.
        while (add-- > 0) {
            comp.execute(GridClientStartNodeTask.class, type);

            tasks.add(comp.future());
        }

        for (ComputeTaskFuture<?> task : tasks)
            task.get();

        // Stop nodes in sequence.
        while (rmv-- > 0)
            parent.compute().execute(GridClientStopNodeTask.class, type);

        // Wait for node stops.
        //U.sleep(1000);

        Collection<String> gridNames = new ArrayList<>();

        for (Ignite g : G.allGrids())
            gridNames.add(g.name());

        parent.log().info(">>> Available grids: " + gridNames);
    }
}
