package org.apache.ignite.console.config;
import java.io.File;

import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.log.Slf4jLog;
import org.eclipse.jetty.webapp.WebAppContext;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

@Component
public class JettyCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {
	
	Slf4jLog logger = new Slf4jLog("com.caucho.quercus");

    @Override
    public void customize(JettyServletWebServerFactory factory) {
        factory.addServerCustomizers(server -> {
        	
        	// 添加一个WebAppContext来加载web.xml
            WebAppContext ctx = new WebAppContext();
            ctx.setContextPath("/webapps"); // 设置上下文路径           
           
            
            // 禁用临时 docbase
        	ctx.setPersistTempDirectory(true);
        	
            // 设置自定义的 ResourceBase
        	if(!"".equals(System.getProperty("quercus.webapp_dir"))) {
        		ctx.setResourceBase("file:"+System.getProperty("quercus.webapp_dir"));
        	}
        	else {
        		ctx.setResourceBase(new File("webapps").getAbsoluteFile().toURI().toString());
        	}
        	
        	ctx.setCopyWebDir(false);

        	ctx.setLogger(logger);

            // 添加WebAppContext到Jetty Server
        	
        	// 创建RewriteHandler
            RewriteHandler rewriteHandler = new RewriteHandler();
            rewriteHandler.setRewriteRequestURI(true);
            rewriteHandler.setRewritePathInfo(false);
            rewriteHandler.setOriginalPathAttribute("requestedPath");
            

            // 添加重写规则
            RewriteRegexRule rewriteRule = new RewriteRegexRule();
            rewriteRule.setRegex("^/webapps/mongoAdmin/([a-zA-Z0-9-_]*)$");
            rewriteRule.setReplacement("/webapps/mongoAdmin/index.php?q=$1");
            rewriteHandler.addRule(rewriteRule);

            // 将RewriteHandler设置为处理器
            rewriteHandler.setHandler(ctx);
            
            // 使用HandlerList组合两个Handler
            HandlerList handlers = new HandlerList();
            handlers.addHandler(rewriteHandler);
            handlers.addHandler(server.getHandler());
            server.setHandler(handlers);
                         
        });
    }
}