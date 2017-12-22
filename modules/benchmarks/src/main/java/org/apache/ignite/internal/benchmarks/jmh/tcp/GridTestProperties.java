package org.apache.ignite.internal.benchmarks.jmh.tcp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Copy from org.apache.ignite.testframework.config.GridTestProperties.
 */
public final class GridTestProperties {
    /** */
    public static final String TESTS_PROP_FILE = "tests.properties";

    /** */
    public static final String TESTS_CFG_PATH = "modules/core/src/test/config";

    /** */
    private static final Pattern PROP_REGEX = Pattern.compile("[@$]\\{[^@${}]+\\}");

    /** */
    private static final Map<String, String> dfltProps;

    /** */
    private static final Map<String, Map<String, String>> pathProps = new HashMap<>();

    /** */
    public static final String MARSH_CLASS_NAME = "marshaller.class";

    /** */
    public static final String ENTRY_PROCESSOR_CLASS_NAME = "entry.processor.class";

    /** Binary marshaller compact footers property. */
    public static final String BINARY_COMPACT_FOOTERS = "binary.marshaller.compact.footers";

    /** "True value" enables {@link BinaryBasicNameMapper} in {@link BinaryTypeConfiguration#getNameMapper()} */
    public static final String BINARY_MARSHALLER_USE_SIMPLE_NAME_MAPPER = "binary.marshaller.use.simple.name.mapper";

    /** */
    static {
        // Initialize IGNITE_HOME system property.
        String igniteHome = System.getProperty("IGNITE_HOME");

        if (igniteHome == null || igniteHome.isEmpty()) {
            igniteHome = System.getenv("IGNITE_HOME");

            if (igniteHome != null && !igniteHome.isEmpty())
                System.setProperty("IGNITE_HOME", igniteHome);
        }

        // Load default properties.
        File cfgFile = getTestConfigurationFile(null, TESTS_PROP_FILE);

        assert cfgFile != null && cfgFile.exists();
        assert !cfgFile.isDirectory();

        dfltProps = Collections.unmodifiableMap(loadFromFile(new HashMap<String, String>(), cfgFile));

        if ("false".equals(System.getProperty("IGNITE_TEST_PROP_DISABLE_LOG4J", "false"))) {
            String user = System.getProperty("user.name");

            assert user != null;
        }
    }

    /**
     * Ensure singleton.
     */
    private GridTestProperties() {
        // No-op.
    }

    /** */
    public static void init() {
        // No-op.
    }

    /**
     * @return Default properties.
     */
    public static synchronized Map<String, String> getDefaultProperties() {
        return dfltProps;
    }

    /**
     * @param name Default property name.
     * @return Default property value.
     */
    public static synchronized String getDefaultProperty(String name) {
        return dfltProps.get(name);
    }

    /**
     * @return Properties.
     */
    public static synchronized Map<String, String> getProperties() {
        String user = System.getProperty("user.name");

        assert user != null;

        return getProperties(user);
    }

    /**
     * @param name Property name.
     * @return Property value.
     */
    public static synchronized String getProperty(String name) {
        return getProperties().get(name);
    }

    /**
     * @param name Property name.
     * @param val Property value.
     */
    public static synchronized void setProperty(String name, String val) {
        getProperties().put(name, val);
    }

    /**
     * @param dir Directory path.
     * @return Properties.
     */
    public static synchronized Map<String, String> getProperties(String dir) {
        Map<String, String> props = pathProps.get(dir);

        if (props == null) {
            props = new HashMap<>();

            // Load default properties.
            props.putAll(dfltProps);

            // Load properties from specified folder
            // potentially overriding defaults.
            loadProperties(props, dir);

            pathProps.put(dir, props);
        }

        return props;
    }

    /**
     * @param name Property name.
     * @param dir Directory path.
     * @return Property value.
     */
    public static synchronized String getProperty(String name, String dir) {
        return getProperties(dir).get(name);
    }

    /**
     * Substitutes environmental or system properties in the given string.
     *
     * @param str String to make substitution in.
     * @return Substituted string.
     */
    private static String substituteProperties(String str) {
        str = str.trim();

        Matcher matcher = PROP_REGEX.matcher(str);

        StringBuffer buf = new StringBuffer();

        while (matcher.find()) {
            String match = matcher.group();

            if (match.length() >= 4) {
                String key = match.substring(2, match.length() - 1);

                String val = System.getenv(key);

                if (val == null)
                    val = System.getProperty(key);

                if (val != null) {
                    // Take care of back slashes.
                    match = val.replaceAll("\\\\", "\\\\\\\\");
                }
                else if (match.startsWith("$"))
                    match = match.replace("$", "\\$");
            }

            matcher.appendReplacement(buf, match);
        }

        matcher.appendTail(buf);

        return buf.toString();
    }

    /**
     * @param props Initial properties.
     * @param dir Directory path.
     * @return Loaded properties.
     */
    private static Map<String, String> loadProperties(Map<String, String> props, String dir) {
        File cfg = getTestConfigurationFile(dir, TESTS_PROP_FILE);

        if (cfg != null)
            loadFromFile(props, cfg);

        return props;
    }

    /**
     * @param user User name.
     * @param fileName File name.
     * @return Configuration file for given user.
     */
    @Nullable private static File getTestConfigurationFile(@Nullable String user, String fileName) {
        String path = TESTS_CFG_PATH;

        if (user != null)
            path += File.separatorChar + user;

        path += File.separatorChar + fileName;

        File file = GridTestUtils.resolveIgnitePath(path);

        if (file != null && file.exists()) {
            assert !file.isDirectory();

            return file;
        }

        return null;
    }

    /**
     * @param props Initial properties.
     * @param file Property file.
     * @return Loaded properties.
     */
    private static Map<String, String> loadFromFile(Map<String, String> props, File file) {
        try {

            try (InputStream in = new FileInputStream(file)) {
                Properties fileProps = new Properties();

                fileProps.load(in);

                for (Map.Entry<Object, Object> prop : fileProps.entrySet())
                    props.put((String)prop.getKey(), (String)prop.getValue());

                for (Map.Entry<String, String> prop : props.entrySet())
                    prop.setValue(substituteProperties(prop.getValue()));
            }
        }
        catch (IOException e) {
            e.printStackTrace();

            assert false : "Failed to load test configuration properties: " + file;
        }

        return props;
    }
}
