package org.apache.ignite.console.jetty.server;

public class IgniteCheckedException extends RuntimeException {
	
	public IgniteCheckedException(String message) {
		super(message);
	}
	
	public IgniteCheckedException(String message,Exception cause) {
		super(message,cause);
	}

}
