package com.shard.jdbc.plugin;

import java.net.URI;

import com.facebook.airlift.configuration.Config;

public class ShardingJdbcConfig {
    private String user;//统一的Metadata用户
    private String password;//统一的Metadata密码
    private String driver;//统一的Metadata数据驱动
    
    
    private String shardingRulePath; //存放sharding文件的本地路径，如果是远程文件，使用下面几个参数
    
    private URI datanode; // 节点配置文件
    private URI shardingRule; // 节点分库规则配置文件
    private URI metadata; //节点分表规则配置文件（多个子表合成一个大表）
   

	/**
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    @Config("jdbc.user")
    public ShardingJdbcConfig setUser(String user) {
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
    @Config("jdbc.password")
    public ShardingJdbcConfig setPassword(String password) {
        this.password = password;
        return this;
    }
    
    /**
     * @return the driver name
     */
    public String getDriver() {
        return driver;
    }

    /**
     * @param set the driver to set
     */
    @Config("jdbc.driver")
    public ShardingJdbcConfig setDriver(String driver) {
        this.driver = driver;
        return this;
    }

    
    /**
     * @return the shardingRulePath 
     */
    public String getShardingRulePath() {
        return shardingRulePath;
    }

    /**
     * @param set the shardingRulePath to set
     */
    @Config("sharding-rule-path")
    public ShardingJdbcConfig setShardingRulePath(String shardingRulePath) {
        this.shardingRulePath = shardingRulePath;
        return this;
    }
    
    /**
     * @return the url
     */
    public URI getDataNodeURI() {
        return this.datanode;
    }

    /**
     * @param url the url to set
     */
    @Config("datanode-uri")
    public ShardingJdbcConfig setDataNodeURI(URI url) {
        this.datanode = url;
        return this;
    }

	
	
    /**
     * @return the url
     */
    public URI getShardingRuleURI() {
        return this.shardingRule;
    }

    /**
     * @param url the url to set
     */
    @Config("sharding-rule-uri")
    public ShardingJdbcConfig setShardingRuleURI(URI url) {
        this.shardingRule = url;
        return this;
    }
    
    /**
     * @return the url
     */
    public URI getMetadataURI() {
        return this.metadata;
    }

    /**
     * @param url the url to set
     */
    @Config("metadata-uri")
    public ShardingJdbcConfig setMetadataURI(URI url) {
        this.metadata = url;
        return this;
    }
}
