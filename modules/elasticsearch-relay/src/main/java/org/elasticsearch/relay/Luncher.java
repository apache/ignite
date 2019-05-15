package org.elasticsearch.relay;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.security.ProtectionDomain;
import java.util.Properties;


import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

public class Luncher {
	private static int port;
	private static int maxThreads;
	private static int minThreads;
	private static int idleTimeout;

	public static void main(String[] args) {
		initJettyConf();
		runJettyServer(ServerConstant.DEFAULT_CONTEXT_PATH);
	}

	/**
	 * 初始化jetty配置
	 */
	private static void initJettyConf() {
		try {
			Properties prop = new Properties();
			InputStream stream = Luncher.class.getResourceAsStream(ServerConstant.basePath);
			if (stream == null) {
				port = ServerConstant.PORT_DEFAULT;
				maxThreads = ServerConstant.MAX_THREADS_DEFAULT;
				minThreads = ServerConstant.MIN_THREADS_DEFAULT;
				idleTimeout = ServerConstant.IDLE_TIMEOUT_DEFAULT;
			} else {
				prop.load(stream);
				port = convertToInt(prop.getProperty(ServerConstant.PORT_STR), ServerConstant.PORT_DEFAULT);
				maxThreads = convertToInt(prop.getProperty(ServerConstant.MAX_THREADS_STR),
						ServerConstant.MAX_THREADS_DEFAULT);
				minThreads = convertToInt(prop.getProperty(ServerConstant.MIN_THREADS_STR),
						ServerConstant.MIN_THREADS_DEFAULT);
				idleTimeout = convertToInt(prop.getProperty(ServerConstant.IDLE_TIMEOUT_STR),
						ServerConstant.IDLE_TIMEOUT_DEFAULT);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * str转int,传入的值不存在,返回默认值
	 * 
	 * @param val
	 *            待转换的值
	 * @param defaultVal
	 *            默认值
	 * @return
	 */
	private static int convertToInt(String val, int defaultVal) {
		if (val==null || val.isEmpty()) {
			return defaultVal;
		}
		return Integer.parseInt(val);
	}

	public static void runJettyServer(String contextPath) {
		Server server = createJettyServer(contextPath);
		try {
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				server.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static Server createJettyServer(String contextPath) {
		// maxThreads=200, minThreads=8, idleTimeout=60s
		ThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
		Server server = new Server(threadPool);
		server.setStopAtShutdown(true);
		ServerConnector connector = new ServerConnector(server);
		connector.setPort(port);
		server.setConnectors(new Connector[] { connector });

		ServletContextHandler servlethandler = new ServletContextHandler();
		servlethandler.setContextPath(contextPath);
		servlethandler.addServlet(ESRelay.class, "/*");
		

		ProtectionDomain protectionDomain = Luncher.class.getProtectionDomain();
		URL location = protectionDomain.getCodeSource().getLocation();

		// 设置work dir,war包将解压到该目录,jsp编译后的文件也将放入其中。
	
		String warFile = location.toExternalForm(); 
		warFile = "target/elasticsearch-relay/"; 
		contextPath = "/";
		WebAppContext context = new	WebAppContext(warFile, contextPath); 
		context.setServer(server);
		
		String currentDir = new File(location.getPath()).getParent(); 
		File workDir = new File(currentDir, "work");
		context.setTempDirectory(workDir); 
		//-context.setClassLoader( Thread.currentThread().getContextClassLoader());  
		

		 // Create a handler list to store our static and servlet context handlers.
		HandlerList handlers = new HandlerList();
		handlers.setHandlers(new Handler[] { context,servlethandler });
	
		// Add the handlers to the server and start jetty.
		//-server.setHandler(handlers);
		server.setHandler(context);

		return server;
	}

	class ServerConstant {
		private static final String DEFAULT_CONTEXT_PATH = "/";
		private static final String basePath = "/conf/jetty-conf.properties";
		private static final String PORT_STR = "port";
		private static final int PORT_DEFAULT = 9200;
		private static final String MAX_THREADS_STR = "maxThreads";
		private static final int MAX_THREADS_DEFAULT = 100;
		private static final String MIN_THREADS_STR = "minThreads";
		private static final int MIN_THREADS_DEFAULT = 4;
		private static final String IDLE_TIMEOUT_STR = "idleTimeout";
		private static final int IDLE_TIMEOUT_DEFAULT = 600000;
	}
}