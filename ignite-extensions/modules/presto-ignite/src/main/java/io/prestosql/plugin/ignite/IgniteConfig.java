package io.prestosql.plugin.ignite;

import io.airlift.configuration.Config;

public class IgniteConfig {
   
    private boolean thinConnection = true;
    private String cfg;


	public boolean isThinConnection() {
		return thinConnection;
	}

	@Config("ignite.thinConnection")
	public IgniteConfig setThinConnection(boolean thinConnection) {
		this.thinConnection = thinConnection;
		return this;
	}
	
	public String getCfg() {
		return cfg;
	}
	
	@Config("ignite.cfg")
	public IgniteConfig setCfg(String cfg) {
		this.cfg = cfg;		
		return this;
	}
}
