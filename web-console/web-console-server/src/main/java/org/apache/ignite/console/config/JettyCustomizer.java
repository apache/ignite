package org.apache.ignite.console.config;
import java.io.File;

import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

@Component
public class JettyCustomizer implements WebServerFactoryCustomizer<JettyServletWebServerFactory> {

    @Override
    public void customize(JettyServletWebServerFactory factory) {
        factory.addServerCustomizers(server -> {
            
            for (org.eclipse.jetty.server.Handler handler : server.getHandlers()) {
                if (handler instanceof org.eclipse.jetty.webapp.WebAppContext) {
                	org.eclipse.jetty.webapp.WebAppContext ctx = (org.eclipse.jetty.webapp.WebAppContext) handler;
                	// 禁用临时 docbase
                	ctx.setPersistTempDirectory(false);
                    // 设置自定义的 ResourceBase
                	if(""!=System.getProperty("quercus.webapp_dir")) {
                		ctx.setResourceBase("file:"+System.getProperty("quercus.webapp_dir"));
                	}
                	else {
                		ctx.setResourceBase(new File(".").getAbsoluteFile().getParentFile().toURI().toString());
                	}
                	
                	ctx.setCopyWebDir(false);
                	ctx.setWelcomeFiles(new String[]{"index.php","index.html"});
                }
            }
        });
    }
}