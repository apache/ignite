package org.shaofan.s3;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.GridKernalContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.WebApplicationInitializer;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

@Component
public class FileManagerInitializer implements WebApplicationInitializer {
	
	public static Ignite ignite;	
	
	
	@Override
	public void onStartup(ServletContext context) throws ServletException {
		GridKernalContext ctx = (GridKernalContext) context.getAttribute("gridKernalContext");
        if(ctx==null) {
        	String configFile = context.getInitParameter("ignite.cfg.path");
        	if(configFile==null) {
        		throw new ServletException("Must set ignite.cfg.path on contenx parameter");
        	}
        	String instanceName = context.getInitParameter("ignite.igfs.instanceName");
        	ignite = Ignition.start(configFile);
        	if(instanceName !=null && Ignition.allGrids().size()>0) {
        		ignite = Ignition.ignite(instanceName);
        	}
        }
        else {
        	ignite = ctx.grid();
        }                
        
        XmlWebApplicationContext appContext = new XmlWebApplicationContext();
        appContext.setConfigLocation("classpath:META-INF/springmvc-servlet.xml");
        
        DispatcherServlet  appServlet =  new DispatcherServlet(appContext);

        ServletRegistration.Dynamic dispatcher = context.addServlet(
                "springmvc", appServlet);
        dispatcher.setLoadOnStartup(1);
        dispatcher.addMapping("/");
        
        MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
        
        dispatcher.setMultipartConfig(MULTI_PART_CONFIG);              
       
        
	}

}
