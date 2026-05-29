package org.apache.ignite.console.agent;
import io.swagger.annotations.ApiOperation;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.console.agent.service.*;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class ServiceDeployment {

    public static List<Class<?>> scanWithSpring(String basePackage) throws IOException {
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        MetadataReaderFactory metaReader = new CachingMetadataReaderFactory();

        String packagePath = basePackage.replace('.', '/');
        Resource[] resources = resolver.getResources("classpath*:" + packagePath + "/**/*.class");

        List<Class<?>> result = new ArrayList<>();
        for (Resource resource : resources) {
            MetadataReader reader = metaReader.getMetadataReader(resource);
            String className = reader.getClassMetadata().getClassName();

            try {
                Class<?> clazz = Class.forName(className);
                for (Method method : clazz.getDeclaredMethods()) {
                    if (method.isAnnotationPresent(ApiOperation.class)) {
                        result.add(clazz);
                        break;
                    }
                }
            } catch (ClassNotFoundException e) {
                // ignore
            }
        }
        return result;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param services Distributed services on the grid.
     */
    public static void deployServices(IgniteServices services) {

        List<Class<?>> serviceList = null;
        try {
            serviceList = scanWithSpring("org.apache.ignite.console.agent.service");
            for(Class svc: serviceList){
                String svcName = svc.getSimpleName();

                try {
                    ApiOperation api = (ApiOperation) svc.getAnnotation(ApiOperation.class);
                    if(api.hidden()){
                        continue;
                    }
                    if(api.nickname()!=null && !api.nickname().isBlank()){
                        svcName = api.nickname();
                    }
                    if(CacheAgentService.class.isAssignableFrom(svc)){
                        CacheAgentService cacheSvc = (CacheAgentService)svc.getDeclaredConstructor().newInstance();
                        services.deployNodeSingleton(svcName,cacheSvc);

                    }
                    else if(ClusterAgentService.class.isAssignableFrom(svc)){
                        ClusterAgentService clusterSvc = (ClusterAgentService)svc.getDeclaredConstructor().newInstance();
                        services.deployClusterSingleton(svcName,clusterSvc);
                    }

                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //String cacheName = "default";
        //services.deployKeyAffinitySingleton("loadDataKeyAffinityService",new ClusterLoadDataService(), cacheName, "id");
    }

}
