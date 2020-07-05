package com.shard.jdbc.database;

/**
 * Created by shun on 2015-12-17 11:49.
 */
public class DbInfo {

    /**
     * dataSourceId
     */
    private String id;
    /**
     * connection url
     */
    private String url;
    /**
     * driver class name
     */
    private String driverClass;
    /**
     * connection username
     */
    private String username;
    /**
     * connection password
     */
    private String password;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
