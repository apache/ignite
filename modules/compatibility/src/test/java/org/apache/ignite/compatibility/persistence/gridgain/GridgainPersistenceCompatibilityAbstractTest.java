package org.apache.ignite.compatibility.persistence.gridgain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.compatibility.persistence.IgnitePersistenceCompatibilityAbstractTest;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Gridgain persistence compatibility abstract test.
 */
@RunWith(Parameterized.class)
public class GridgainPersistenceCompatibilityAbstractTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** GridGain group id. */
    private static final String GRIDGAIN_GROUP_ID = "org.gridgain";

    /** GridGain version. */
    @Parameterized.Parameter
    public String ggVer;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "ggVer={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {"8.7.3"},
            {"8.7.4"},
            {"8.7.5"},
            {"8.7.6"},
            {"8.7.7"},
            {"8.7.8"},
            {"8.7.9"},
            {"8.7.10"},
            {"8.7.11"},
            {"8.7.12"},
            {"8.7.13"},
            {"8.7.14"},
            {"8.7.15"},
            {"8.7.16"},
            {"8.7.17"},
            {"8.7.18"},
            {"8.7.19"}
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
            ));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        final Collection<Dependency> dependencies = new ArrayList<>();

        dependencies.add(new Dependency("core", GRIDGAIN_GROUP_ID, "ignite-core", null, false));
        dependencies.add(new Dependency("core", GRIDGAIN_GROUP_ID, "ignite-core", null, true));

        return dependencies;
    }

    /**
     * @param v1 the first version to be compared.
     * @param v2 the second version to be compared.
     * @return a negative integer, zero, or a positive integer as the
     *         first version is less than, equal to, or greater than the
     *         second.
     */
    public static int compareVersions(String v1, String v2) {
        if (v1.equals(v2))
            return 0;

        String dot = "\\.";
        String dash = "-";

        String[] ver1 = v1.split(dot, 3);
        String[] ver2 = v2.split(dot, 3);

        assert ver1.length == 3;
        assert ver2.length == 3;

        if (Integer.parseInt(ver1[0]) >= Integer.parseInt(ver2[0]) &&
            Integer.parseInt(ver1[1]) >= Integer.parseInt(ver2[1]) &&
            Integer.parseInt(ver1[2].split(dash)[0]) >= Integer.parseInt(ver2[2].split(dash)[0]))

            return 1;
        else
            return -1;
    }
}
