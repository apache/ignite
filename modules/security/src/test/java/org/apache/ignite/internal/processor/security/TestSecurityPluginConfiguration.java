package org.apache.ignite.internal.processor.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 * Security configuration for test.
 */
public class TestSecurityPluginConfiguration implements PluginConfiguration {
    /** Default test security processor class name. */
    public static final String DFLT_TEST_SECURITY_PROCESSOR_CLS_NAME =
        "org.apache.ignite.internal.processor.security.TestSecurityProcessor";

    private TestSecurityData nodeSecData = new TestSecurityData();

    private Collection<TestSecurityData> clientsSecData = new ArrayList<>();

    /** Security processor class name. */
    private String secProcCls;

    /**
     * Getting security permission set.
     */
    public SecurityPermissionSet getPermissions() {
        return nodeSecData.getPermissions();
    }

    /**
     * @param prmSet Security permission set.
     */
    public TestSecurityPluginConfiguration setPermissions(SecurityPermissionSet prmSet) {
        nodeSecData.setPermissions(prmSet);

        return this;
    }

    /**
     * Login.
     */
    public String getLogin() {
        return nodeSecData.getLogin();
    }

    /**
     * @param login Login.
     */
    public TestSecurityPluginConfiguration setLogin(String login) {
        nodeSecData.setLogin(login);

        return this;
    }

    /**
     * Password.
     */
    public String getPwd() {
        return nodeSecData.getPwd();
    }

    /**
     * @param pwd Password.
     */
    public TestSecurityPluginConfiguration setPwd(String pwd) {
        nodeSecData.setPwd(pwd);

        return this;
    }

    public TestSecurityPluginConfiguration nodeSecData(TestSecurityData nodeSecData) {
        this.nodeSecData = nodeSecData;

        return this;
    }

    public TestSecurityData nodeSecData() {
        return nodeSecData;
    }

    public TestSecurityPluginConfiguration clientSecData(TestSecurityData... data) {
        clientsSecData.addAll(Arrays.asList(data));

        return this;
    }

    public Collection<TestSecurityData> clientsSecData() {
        return clientsSecData;
    }

    /**
     * Getting security processor class name.
     */
    public String getSecurityProcessorClass() {
        if (F.isEmpty(secProcCls))
            return DFLT_TEST_SECURITY_PROCESSOR_CLS_NAME;

        return secProcCls;
    }

    /**
     * @param secProcCls Security processor class name.
     */
    public TestSecurityPluginConfiguration setSecurityProcessorClass(String secProcCls) {
        this.secProcCls = secProcCls;

        return this;
    }
}