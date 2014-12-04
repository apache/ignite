/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nodestart;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
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
    public static IgniteBiTuple<Collection<Map<String, Object>>, Map<String, Object>> parseFile(
        File file) throws GridException {
        assert file != null;
        assert file.exists();
        assert file.isFile();

        BufferedReader br = null;

        int lineCnt = 1;

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

            String section = null;

            Collection<Map<String, Object>> hosts = new LinkedList<>();
            Map<String, Object> dflts = null;
            Map<String, Object> props = null;

            for (String line; (line = br.readLine()) != null; lineCnt++) {
                String l = line.trim();

                if (l.isEmpty() || l.startsWith("#") || l.startsWith(";"))
                    continue;

                if (l.startsWith("[") && l.endsWith("]")) {
                    Map<String, Object> dfltsTmp = processSection(section, hosts, dflts, props);

                    if (dfltsTmp != null)
                        dflts = dfltsTmp;

                    props = new HashMap<>();

                    section = l.substring(1, l.length() - 1);
                }
                else if (l.contains("=")) {
                    if (section == null)
                        throw new GridException("GridGain ini format doesn't support unnamed section.");

                    String key = l.substring(0, l.indexOf('='));
                    String val = line.substring(line.indexOf('=') + 1);

                    switch (key) {
                        case HOST:
                        case UNAME:
                        case PASSWD:
                        case GG_HOME:
                        case CFG:
                        case SCRIPT:
                            props.put(key, val);
                            break;

                        case PORT:
                        case NODES:
                            props.put(key, Integer.valueOf(val));
                            break;

                        case KEY:
                            props.put(KEY, new File(val));
                            break;
                    }
                }
                else
                    throw new GridException("Failed to parse INI file (line " + lineCnt + ").");
            }

            Map<String, Object> dfltsTmp = processSection(section, hosts, dflts, props);

            if (dfltsTmp != null)
                dflts = dfltsTmp;

            return F.t(hosts, dflts);
        }
        catch (IOException | NumberFormatException e) {
            throw new GridException("Failed to parse INI file (line " + lineCnt + ").", e);
        }
        finally {
            U.closeQuiet(br);
        }
    }

    /**
     * Processes section of parsed INI file.
     *
     * @param section Name of the section.
     * @param hosts Already parsed properties for sections excluding default.
     * @param dflts Parsed properties for default section.
     * @param props Current properties.
     * @return Default properties if specified section is default, {@code null} otherwise.
     * @throws GridException If INI file contains several default sections.
     */
    private static Map<String, Object> processSection(String section, Collection<Map<String, Object>> hosts,
        Map<String, Object> dflts, Map<String, Object> props) throws GridException {
        if (section == null || props == null)
            return null;

        if (DFLT_SECTION.equalsIgnoreCase(section)) {
            if (dflts != null)
                throw new GridException("Only one '" + DFLT_SECTION + "' section is allowed.");

            return props;
        }
        else {
            hosts.add(props);

            return null;
        }
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
        Map<String, Collection<GridRemoteStartSpecification>> specsMap = U.newHashMap(hosts.size());

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
        GridLogger log = null;

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
                log = (GridLogger)dflts.get(LOGGER);
        }

        if (port <= 0)
            throw new GridException("Invalid port number: " + port);

        if (nodes <= 0)
            throw new GridException("Invalid number of nodes: " + nodes);

        return new GridRemoteStartSpecification(null, port, uname, passwd,
            key, nodes, ggHome, cfg, script, log);
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
