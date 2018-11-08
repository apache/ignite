package org.apache.ignite.internal.processor.security;

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

    /** Security permission set. */
    private SecurityPermissionSet prmSet;

    /** Login. */
    private String login;

    /** Password. */
    private String pwd;

    /** User object. */
    private Object userObj;

    /** Security processor class name. */
    private String secProcCls;

    /**
     * Getting security permission set.
     */
    public SecurityPermissionSet getPermissions() {
        return prmSet;
    }

    /**
     * @param prmSet Security permission set.
     */
    public TestSecurityPluginConfiguration setPermissions(SecurityPermissionSet prmSet) {
        this.prmSet = prmSet;

        return this;
    }

    /**
     * Login.
     */
    public String getLogin() {
        return login;
    }

    /**
     * @param login Login.
     */
    public TestSecurityPluginConfiguration setLogin(String login) {
        this.login = login;

        return this;
    }

    /**
     * Password.
     */
    public String getPwd() {
        return pwd;
    }

    /**
     * @param pwd Password.
     */
    public TestSecurityPluginConfiguration setPwd(String pwd) {
        this.pwd = pwd;

        return this;
    }

    /**
     * User object.
     */
    public Object getUserObj() {
        return userObj;
    }

    /**
     * @param userObj User object.
     */
    public TestSecurityPluginConfiguration setUserObj(Object userObj) {
        this.userObj = userObj;

        return this;
    }

    /**
     * Getting security processor class name.
     */
    public String getSecurityProcessorClass() {
        if(F.isEmpty(secProcCls))
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