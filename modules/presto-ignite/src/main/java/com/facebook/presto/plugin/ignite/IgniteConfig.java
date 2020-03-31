package com.facebook.presto.plugin.ignite;

import io.airlift.configuration.Config;

public class IgniteConfig {
    private String user;
    private String password;
    private String url;
    private boolean thinConnection;
    private String cfg;
   

	/**
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    @Config("ignite.user")
    public IgniteConfig setUser(String user) {
        this.user = user;
        return this;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    @Config("ignite.password")
    public IgniteConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * @return the url
     */
    public String getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     */
    @Config("ignite.password")
    public IgniteConfig setUrl(String url) {
        this.url = url;
        return this;
    }

	public boolean isThinConnection() {
		return thinConnection;
	}

	@Config("ignite.thinConnection")
	public void setThinConnection(boolean thinConnection) {
		this.thinConnection = thinConnection;
	}
	
	public String getCfg() {
		return cfg;
	}
	@Config("ignite.cfg")
	public void setCfg(String cfg) {
		this.cfg = cfg;
	}
}
