package org.apache.ignite.internal.processor.security;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.plugin.security.TestSecurityPluginConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Common class for security tests.
 */
public class AbstractSecurityTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(
            TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS,
            "org.apache.ignite.internal.processor.security.TestSecurityProcessor"
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param instanceName Instance name.
     * @param login Login.
     * @param pwd Password.
     * @param userObj User object.
     * @param prmSet Security permission set.
     */
    protected IgniteConfiguration getConfiguration(String instanceName,
        String login, String pwd, Object userObj, SecurityPermissionSet prmSet) throws Exception {

        return getConfiguration(instanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setPluginConfigurations(
                new TestSecurityPluginConfiguration()
                    .setLogin(login)
                    .setPwd(pwd)
                    .setUserObj(userObj)
                    .setPermissions(prmSet)
            );
    }

    /**
     * @param idx Index.
     * @param login Login.
     * @param pwd Password.
     * @param userObj User object.
     * @param prmSet Security permission set.
     */
    protected IgniteConfiguration getConfiguration(int idx, String login, String pwd, Object userObj,
        SecurityPermissionSet prmSet) throws Exception {
        return getConfiguration(getTestIgniteInstanceName(idx), login, pwd, userObj, prmSet);
    }

    /**
     * @param idx Index.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(int idx, String login, String pwd, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(getConfiguration(idx, login, pwd, null, prmSet));
    }

    /**
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(String login, String pwd, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(login, pwd, prmSet, false);
    }

    /**
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(String login, String pwd, SecurityPermissionSet prmSet,
        boolean isClient) throws Exception {
        return startGrid(
            getConfiguration(G.allGrids().size(), login, pwd, null, prmSet).setClientMode(isClient)
        );
    }

    /**
     * @param userObj User object.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(Object userObj, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(userObj, prmSet, false);
    }

    /**
     * @param userObj User object.
     * @param prmSet Security permission set.
     * @param isClient Is client.
     */
    protected IgniteEx startGrid(Object userObj, SecurityPermissionSet prmSet, boolean isClient)
        throws Exception {
        return startGrid(
            getConfiguration(G.allGrids().size(), null, null, userObj, prmSet).setClientMode(isClient)
        );
    }

    /**
     * @param instanceName Instance name.
     * @param login Login.
     * @param pwd Password.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(String instanceName, String login, String pwd,
        SecurityPermissionSet prmSet) throws Exception {
        return startGrid(getConfiguration(instanceName, login, pwd, null, prmSet));
    }

    /**
     * @param idx Index.
     * @param userObj User object.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(int idx, Object userObj, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(getConfiguration(idx, null, null, userObj, prmSet));
    }

    /**
     * @param instanceName Instance name.
     * @param userObj User object.
     * @param prmSet Security permission set.
     */
    protected IgniteEx startGrid(String instanceName, Object userObj, SecurityPermissionSet prmSet) throws Exception {
        return startGrid(getConfiguration(instanceName, null, null, userObj, prmSet));
    }

    /**
     * Getting security permission set builder.
     */
    protected SecurityPermissionSetBuilder builder() {
        return SecurityPermissionSetBuilder.create().defaultAllowAll(true);
    }

    /**
     * Getting allow all security permissions.
     */
    protected SecurityPermissionSet allowAll() {
        return builder().build();
    }
}
