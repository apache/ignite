/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nodestart;

import org.apache.commons.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Util methods for {@code GridProjection.startNodes(..)} methods.
 */
public class GridNodeStartUtils {
    /** Key for hostname. */
    public static final String HOST = "host";

    /** Key for port number. */
    public static final String PORT = "port";

    /** Key for username. */
    public static final String UNAME = "uname";

    /** Key for password. */
    public static final String PASSWD = "passwd";

    /** Key for private key file. */
    public static final String KEY = "key";

    /** Key for number of nodes. */
    public static final String NODES = "nodes";

    /** Key for GridGain home folder. */
    public static final String GG_HOME = "ggHome";

    /** Key for configuration path. */
    public static final String CFG = "cfg";

    /** Key for script path. */
    public static final String SCRIPT = "script";

    /** Key for logger. */
    public static final String LOGGER = "logger";

    /** Default connection timeout. */
    public static final int DFLT_TIMEOUT = 10000;

    /** Default maximum number of parallel connections. */
    public static final int DFLT_MAX_CONN = 5;

    /** Symbol that specifies range of IPs. */
    private static final String RANGE_SMB = "~";

    /** Default port. */
    private static final int DFLT_PORT = 22;

    /** Default number of nodes. */
    private static final int DFLT_NODES = 1;

    /** Default configuration path. */
    private static final String DFLT_CFG = "";

    /** Defaults section name. */
    private static final String DFLT_SECTION = "defaults";

    /**
     * Ensure singleton.
     */
    private GridNodeStartUtils() {
        // No-op.
    }

    /**
     * Parses INI file.
     *
     * @param file File.
     * @return Tuple with host maps and default values.
     * @throws GridException In case of error.
     */
    public static GridBiTuple<Collection<Map<String, Object>>, Map<String, Object>> parseFile(
        File file) throws GridException {
        assert file != null;
        assert file.exists();
        assert file.isFile();

        Collection<Map<String, Object>> hosts = new LinkedList<>();
        Map<String, Object> dflts = null;

        try {
            HierarchicalINIConfiguration ini = new HierarchicalINIConfiguration(file);

            Set<String> sections = ini.getSections();

            for (String sectionName : sections) {
                Map<String, Object> props = new HashMap<>();

                SubnodeConfiguration section = ini.getSection(sectionName);

                props.put(HOST, section.getString(HOST));
                props.put(PORT, section.getInteger(PORT, null));
                props.put(UNAME, section.getString(UNAME));
                props.put(PASSWD, section.getString(PASSWD));

                if (section.getString(KEY) != null)
                    props.put(KEY, new File(section.getString(KEY)));

                props.put(NODES, section.getInteger(NODES, null));
                props.put(GG_HOME, section.getString(GG_HOME));
                props.put(CFG, section.getString(CFG));
                props.put(SCRIPT, section.getString(SCRIPT));

                if (DFLT_SECTION.equalsIgnoreCase(sectionName)) {
                    if (dflts != null)
                        throw new GridException("Only one '" + DFLT_SECTION + "' section is allowed.");

                    dflts = props;
                }
                else
                    hosts.add(props);
            }
        }
        catch (ConfigurationException e) {
            throw new GridException("Failed to parse INI file.", e);
        }

        return F.t(hosts, dflts);
    }

    /**
     * Makes specifications.
     *
     * @param hosts Host configurations.
     * @param dflts Default values.
     * @return Specification grouped by hosts.
     * @throws GridException In case of error.
     */
    public static Map<String, Collection<GridRemoteStartSpecification>> specifications(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts)
        throws GridException {
        Map<String, Collection<GridRemoteStartSpecification>> specsMap =
            new HashMap<>(hosts.size());

        GridRemoteStartSpecification dfltSpec = processDefaults(dflts);

        for (Map<String, Object> host : hosts) {
            Collection<GridRemoteStartSpecification> specs = processHost(host, dfltSpec);

            for (GridRemoteStartSpecification spec : specs)
                F.addIfAbsent(specsMap, spec.host(), new Callable<Collection<GridRemoteStartSpecification>>() {
                    @Override public Collection<GridRemoteStartSpecification> call() throws Exception {
                        return new HashSet<>();
                    }
                }).add(spec);
        }

        return specsMap;
    }

    /**
     * Converts properties map to default specification.
     *
     * @param dflts Properties.
     * @return Specification.
     * @throws GridException If properties are invalid.
     */
    private static GridRemoteStartSpecification processDefaults(@Nullable Map<String, Object> dflts)
        throws GridException {
        int port = DFLT_PORT;
        String uname = System.getProperty("user.name");
        String passwd = null;
        File key = null;
        int nodes = DFLT_NODES;
        String ggHome = null;
        String cfg = DFLT_CFG;
        String script = null;
        GridLogger logger = null;

        if (dflts != null) {
            if (dflts.get(PORT) != null)
                port = (Integer)dflts.get(PORT);

            if (dflts.get(UNAME) != null)
                uname = (String)dflts.get(UNAME);

            if (dflts.get(PASSWD) != null)
                passwd = (String)dflts.get(PASSWD);

            if (dflts.get(KEY) != null)
                key = (File)dflts.get(KEY);

            if (dflts.get(NODES) != null)
                nodes = (Integer)dflts.get(NODES);

            if (dflts.get(GG_HOME) != null)
                ggHome = (String)dflts.get(GG_HOME);

            if (dflts.get(CFG) != null)
                cfg = (String)dflts.get(CFG);

            if (dflts.get(SCRIPT) != null)
                script = (String)dflts.get(SCRIPT);

            if (dflts.get(LOGGER) != null)
                logger = (GridLogger)dflts.get(LOGGER);
        }

        if (port <= 0)
            throw new GridException("Invalid port number: " + port);

        if (nodes <= 0)
            throw new GridException("Invalid number of nodes: " + nodes);

        return new GridRemoteStartSpecification(null, port, uname, passwd,
            key, nodes, ggHome, cfg, script, logger);
    }

    /**
     * Converts properties map to specification.
     *
     * @param props Properties.
     * @param dfltSpec Default specification.
     * @return Specification.
     * @throws GridException If properties are invalid.
     */
    private static Collection<GridRemoteStartSpecification> processHost(Map<String, Object> props,
        GridRemoteStartSpecification dfltSpec) throws GridException {
        assert props != null;
        assert dfltSpec != null;

        if (props.get(HOST) == null)
            throw new GridException("Host must be specified.");

        Set<String> hosts = expandHost((String)props.get(HOST));
        int port = props.get(PORT) != null ? (Integer)props.get(PORT) : dfltSpec.port();
        String uname = props.get(UNAME) != null ? (String)props.get(UNAME) : dfltSpec.username();
        String passwd = props.get(PASSWD) != null ? (String)props.get(PASSWD) : dfltSpec.password();
        File key = props.get(KEY) != null ? (File)props.get(KEY) : dfltSpec.key();
        int nodes = props.get(NODES) != null ? (Integer)props.get(NODES) : dfltSpec.nodes();
        String ggHome = props.get(GG_HOME) != null ? (String)props.get(GG_HOME) : dfltSpec.ggHome();
        String cfg = props.get(CFG) != null ? (String)props.get(CFG) : dfltSpec.configuration();
        String script = props.get(SCRIPT) != null ? (String)props.get(SCRIPT) : dfltSpec.script();

        if (port<= 0)
            throw new GridException("Invalid port number: " + port);

        if (nodes <= 0)
            throw new GridException("Invalid number of nodes: " + nodes);

        if (passwd == null && key == null)
            throw new GridException("Password or private key file must be specified.");

        if (passwd != null && key != null)
            passwd = null;

        Collection<GridRemoteStartSpecification> specs =
            new ArrayList<>(hosts.size());

        for (String host : hosts)
            specs.add(new GridRemoteStartSpecification(host, port, uname, passwd,
                key, nodes, ggHome, cfg, script, dfltSpec.logger()));

        return specs;
    }

    /**
     * Parses and expands range of IPs, if needed. Host names without the range
     * returned as is.
     *
     * @param addr Host with or without `~` range.
     * @return Set of individual host names (IPs).
     * @throws GridException In case of error.
     */
    public static Set<String> expandHost(String addr) throws GridException {
        assert addr != null;

        Set<String> addrs = new HashSet<>();

        if (addr.contains(RANGE_SMB)) {
            String[] parts = addr.split(RANGE_SMB);

            if (parts.length != 2)
                throw new GridException("Invalid IP range: " + addr);

            int lastDot = parts[0].lastIndexOf('.');

            if (lastDot < 0)
                throw new GridException("Invalid IP range: " + addr);

            String base = parts[0].substring(0, lastDot);
            String begin = parts[0].substring(lastDot + 1);
            String end = parts[1];

            try {
                int a = Integer.valueOf(begin);
                int b = Integer.valueOf(end);

                if (a > b)
                    throw new GridException("Invalid IP range: " + addr);

                for (int i = a; i <= b; i++)
                    addrs.add(base + "." + i);
            }
            catch (NumberFormatException e) {
                throw new GridException("Invalid IP range: " + addr, e);
            }
        }
        else
            addrs.add(addr);

        return addrs;
    }
}
