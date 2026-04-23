package org.apache.ignite.console.config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;
import java.io.File;


@Component
public class JettyCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {
    private static final Logger logger = LogManager.getLogger(JettyCustomizer.class);

    @Override
    public void customize(JettyServletWebServerFactory factory) {
        factory.addServerCustomizers(server -> {
            ContextHandlerCollection handlers = new ContextHandlerCollection();

            // 保留 Spring Boot 的默认 Handler
            handlers.setHandlers(server.getHandlers());

            // 扫描 webapps 目录下的所有子目录
            File webappsDir = new File("./webapps");
            if (webappsDir.exists() && webappsDir.isDirectory()) {
                for (File appDir : webappsDir.listFiles(File::isDirectory)) {
                    logger.info("try to deploy webapp: {}", appDir.getName());
                    WebAppContext webapp = new WebAppContext();
                    webapp.setContextPath("/" + appDir.getName());
                    webapp.setWar(appDir.getAbsolutePath());
                    webapp.setParentLoaderPriority(true);
                    webapp.setWelcomeFiles(new String[]{"index.php","index.jsp","index.html"});

                    webapp.setErrorHandler(new ErrorHandler());

                    if(appDir.getName().equals("mongoAdmin")){

                        RewriteHandler rewriteHandler = new RewriteHandler();
                        rewriteHandler.setOriginalPathAttribute("requestedPath");

                        // 添加重写规则
                        RewriteRegexRule rewriteRule = new RewriteRegexRule();
                        rewriteRule.setRegex("^/mongoAdmin/([a-zA-Z0-9-_]*)$");
                        rewriteRule.setReplacement("/mongoAdmin/index.php?q=$1");
                        rewriteHandler.addRule(rewriteRule);

                        // 将RewriteHandler设置为处理器
                        rewriteHandler.setHandler(webapp);

                        // 2. 将重写处理器设置为服务器根处理器
                        handlers.addHandler(rewriteHandler);
                    }

                    handlers.addHandler(webapp);
                }
            }
            server.setHandler(handlers);
        });
    }
}