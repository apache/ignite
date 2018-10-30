package org.apache.ignite.internal.processor.security;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processor.security.DefaultTestSecurityProcessor.USER_SECURITY_TOKEN;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class DefaultTestSecurityProcessorSelfTest extends GridCommonAbstractTest {
    /** Cache name for tests. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Cache name for tests. */
    private static final String SEC_CACHE_NAME = "SECOND_TEST_CACHE";

    /** */
    protected IgniteEx succsessSrv;

    /** */
    protected IgniteEx succsessClnt;

    /** */
    protected IgniteEx failSrv;

    /** */
    protected IgniteEx failClnt;

    public void test() {
        succsessSrv.cache(CACHE_NAME).put("key", 1);

        Throwable exception = null;
        try{
            failSrv.cache(CACHE_NAME).put("fail_key", -1);

        }catch (Throwable e){
            exception = e;
        }

        assertThat(exception, notNullValue());

        assertThat(succsessSrv.cache(CACHE_NAME).get("key"), is(1));

        assertThat(succsessSrv.cache(CACHE_NAME).get("fail_key"), nullValue());

    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(
            TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS,
            "org.apache.ignite.internal.processor.security.DefaultTestSecurityProcessor"
        );

        SecurityPermissionProvider.add(
            "success_server",
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, CACHE_PUT, CACHE_REMOVE, CACHE_READ)
                .appendCachePermissions(SEC_CACHE_NAME, CACHE_PUT, CACHE_REMOVE, CACHE_READ)
                .build()
        );
        SecurityPermissionProvider.add(
            "fail_server",
            SecurityPermissionSetBuilder.create()
                .defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME)
                .appendCachePermissions(SEC_CACHE_NAME)
                .build()
        );

        succsessSrv = startGrid("success_server");

//        succsessClnt = startGrid(getConfiguration("success_client").setClientMode(true));

        failSrv = startGrid("fail_server");

//        failClnt = startGrid(getConfiguration("fail_client").setClientMode(true));

        succsessSrv.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        Map<String, String> userAttrs = new HashMap<>();

        userAttrs.put(USER_SECURITY_TOKEN, igniteInstanceName);

        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setUserAttributes(userAttrs)
            .setCacheConfiguration(getCacheConfigurations());
    }

    /**
     * Getting array of cache configurations.
     */
    protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<>()
                .setName(SEC_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
        };
    }

}
