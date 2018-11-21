package org.apache.ignite.internal.processor.security;

import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

public class TestSecurityData {
    /** Login. */
    private String login;

    /** Password. */
    private String pwd;

    /** Security permission set. */
    private SecurityPermissionSet prmSet;

    public TestSecurityData() {
        // No-op.
    }

    public TestSecurityData(String login, String pwd, SecurityPermissionSet prmSet) {
        this.login = login;
        this.pwd = pwd;
        this.prmSet = prmSet;
    }

    /**
     * Getting security permission set.
     */
    public SecurityPermissionSet getPermissions() {
        return prmSet;
    }

    /**
     * @param prmSet Security permission set.
     */
    public TestSecurityData setPermissions(SecurityPermissionSet prmSet) {
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
    public TestSecurityData setLogin(String login) {
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
    public TestSecurityData setPwd(String pwd) {
        this.pwd = pwd;

        return this;
    }

    public SecurityCredentials credentials() {
        return new SecurityCredentials(getLogin(), getPwd(), null);
    }
}
