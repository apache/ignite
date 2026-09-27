package org.apache.ignite.console.config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.util.resource.PathResource;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.resource.URLResourceFactory;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;

import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


@Component
public class JettyCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {
    private static final Logger logger = LogManager.getLogger(JettyCustomizer.class);

    @Override
    public void customize(JettyServletWebServerFactory factory) {
        System.setProperty("org.eclipse.jetty.util.FileResource.checkAliases", "true");
        System.setProperty("org.eclipse.jetty.webapp.Configuration.discover", "false");

        factory.addServerCustomizers(server -> {
            ContextHandlerCollection handlers = new ContextHandlerCollection();

            // 保留 Spring Boot 的默认 Handler
            handlers.setHandlers(server.getHandlers());

            // 扫描 webapps 目录下的所有子目录
            File webappsDir = new File("./webapps");
            if (webappsDir.exists() && webappsDir.isDirectory()) {
                for (File appDir : webappsDir.listFiles(File::isDirectory)) {
                    try {
                        logger.info("try to deploy webapp: {}", appDir.getName());
                        WebAppContext webapp = new WebAppContext();
                        webapp.setContextPath("/" + appDir.getName());
                        webapp.setDisplayName(appDir.getName());
                        webapp.setBaseResource(ResourceFactory.root().newResource(appDir.getCanonicalPath()));

                        webapp.setWar(appDir.getCanonicalPath());
                        webapp.setParentLoaderPriority(true);
                        webapp.setWelcomeFiles(new String[]{"index.php","index.jsp","index.html"});
                        webapp.setErrorHandler(new ErrorHandler());

                        createDevelopmentWebAppContext(webapp);

                        RewriteHandler rewriteHandler = checkRewriteRule(webapp);

                        if(rewriteHandler!=null){
                            // 将RewriteHandler设置为处理器
                            rewriteHandler.setHandler(webapp);

                            // 2. 将重写处理器设置为服务器根处理器
                            handlers.addHandler(rewriteHandler);
                        }
                        else {
                            handlers.addHandler(webapp);
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
            server.setHandler(handlers);
        });
    }

    /**
     * 方案一：直接使用原始目录 - 不锁文件 + 实时检测静态文件变化
     */
    private WebAppContext createDevelopmentWebAppContext(WebAppContext webapp) {

        // 禁用 WAR 解压和文件复制（防止文件锁）
        webapp.setCopyWebDir(false);
        webapp.setCopyWebInf(false);
        webapp.setExtractWAR(false);

        // 禁用文件锁定的关键配置
        webapp.getInitParams().put("org.eclipse.jetty.util.FileResource.checkAliases", "false");
        webapp.getInitParams().put("org.eclipse.jetty.webapp.Configuration.Discover", "true");

        // 关键：启用静态文件的实时检测
        webapp.setInitParameter("org.eclipse.jetty.servlet.Default.useFileMappedBuffer", "false");

        // 设置资源刷新间隔（单位：秒，0表示每次都检查）
        webapp.setInitParameter("org.eclipse.jetty.servlet.Default.resourceCache", "10");

        // 启用开发模式 - 自动扫描文件变化
        webapp.setInitParameter("org.eclipse.jetty.webapp.Configuration.discover", "true");
        webapp.setInitParameter("org.eclipse.jetty.webapp.configuration", "org.eclipse.jetty.webapp.WebInfConfiguration");

        logger.info("Development mode enabled for {}: static files will be detected in real-time", webapp.getDisplayName());

        return webapp;
    }

    private RewriteHandler checkRewriteRule(WebAppContext webapp ){
        File rewriteXml = new File(webapp.getWar(),"WEB-INF/rewrite.xml");
        if(rewriteXml.exists()) {
            RewriteHandler rewriteHandler = new RewriteHandler();
            rewriteHandler.setOriginalPathAttribute("requestedPath");

            try {
                loadRewriteRulesFromXml(rewriteHandler,rewriteXml.getPath());
            } catch (Exception e) {
                logger.error("解析WEB-INF/rewrite.xml失败",e.getMessage());
                // 添加重写规则
                String appName = webapp.getContextPath();
                RewriteRegexRule rewriteRule = new RewriteRegexRule();
                rewriteRule.setRegex("^"+appName+"/([a-zA-Z0-9-_]*)");
                rewriteRule.setAddQueries(true);
                rewriteRule.setReplacement(appName+"/index.php?q=$1");
                rewriteHandler.addRule(rewriteRule);
            }
            return rewriteHandler;
        }
        return null;
    }

    /**
     * 从 XML 配置文件加载 Rewrite 规则
     * @param rewriteHandler 要配置的 RewriteHandler 实例
     * @param xmlPath XML 配置文件路径（classpath 或文件系统）
     */
    private static void loadRewriteRulesFromXml(RewriteHandler rewriteHandler, String xmlPath) throws Exception {
        // 方式1：从 classpath 加载（推荐用于打包后的应用）
        URL xmlUrl;
        Path xmlFile = Paths.get(xmlPath);
        if (xmlFile.toFile().exists()) {
            xmlUrl = xmlFile.toUri().toURL();
        } else {
            throw new IllegalArgumentException("XML configuration file not found: " + xmlPath);
        }

        System.out.println("Loading rewrite configuration from: " + xmlUrl);

        // 关键：使用 Jetty 的 XmlConfiguration 解析 XML
        XmlConfiguration configuration = new XmlConfiguration(urlResourceFactory.newResource(xmlUrl),Map.of("rewriteHandler",rewriteHandler),Map.of());

        // 执行配置
        configuration.configure(rewriteHandler);

        System.out.println("Rewrite rules loaded successfully!");
        System.out.println("Total rules: " + rewriteHandler.getRules().size());
    }

    private static URLResourceFactory urlResourceFactory = new URLResourceFactory();
}