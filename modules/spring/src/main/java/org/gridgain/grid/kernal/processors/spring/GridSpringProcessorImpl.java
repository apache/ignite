/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.spring;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.*;
import org.springframework.beans.factory.config.*;
import org.springframework.beans.factory.support.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Spring configuration processor.
 */
public class GridSpringProcessorImpl implements GridSpringProcessor {
    /** Path to {@code gridgain.xml} file. */
    public static final String GRIDGAIN_XML_PATH = "META-INF/gridgain.xml";

    /** System class loader user version. */
    private static final AtomicReference<String> SYS_LDR_VER = new AtomicReference<>(null);

    /**
     * Try to execute LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", null)
     * to turn off default logging for Spring Framework.
     */
    static {
        Class<?> logFactoryCls = null;

        try {
            logFactoryCls = Class.forName("org.apache.commons.logging.LogFactory");
        }
        catch (ClassNotFoundException ignored) {
            // No-op.
        }

        if (logFactoryCls != null) {
            try {
                Object factory = logFactoryCls.getMethod("getFactory").invoke(null);

                factory.getClass().getMethod("setAttribute", String.class, Object.class).
                    invoke(factory, "org.apache.commons.logging.Log", null);
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> loadConfigurations(
        URL cfgUrl, String... excludedProps) throws GridException {
        ApplicationContext springCtx;

        try {
            springCtx = applicationContext(cfgUrl, excludedProps);
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new GridException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + cfgUrl + ']', e);
            else
                throw new GridException("Failed to instantiate Spring XML application context [springUrl=" +
                    cfgUrl + ", err=" + e.getMessage() + ']', e);
        }

        Map<String, IgniteConfiguration> cfgMap;

        try {
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new GridException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null || cfgMap.isEmpty())
            throw new GridException("Failed to find grid configuration in: " + cfgUrl);

        return F.t(cfgMap.values(), new GridSpringResourceContextImpl(springCtx));
    }

    /** {@inheritDoc} */
    @Override public Map<Class<?>, Object> loadBeans(URL cfgUrl, Class<?>... beanClasses) throws GridException {
        assert beanClasses.length > 0;

        GenericApplicationContext springCtx;

        try {
            springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(cfgUrl));

            springCtx.refresh();
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new GridException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + cfgUrl + ']', e);
            else
                throw new GridException("Failed to instantiate Spring XML application context [springUrl=" +
                    cfgUrl + ", err=" + e.getMessage() + ']', e);
        }

        Map<Class<?>, Object> beans = new HashMap<>();

        for (Class<?> cls : beanClasses)
            beans.put(cls, bean(springCtx, cls));

        return beans;
    }

    /** {@inheritDoc} */
    @Override public String userVersion(ClassLoader ldr, GridLogger log) {
        assert ldr != null;
        assert log != null;

        // For system class loader return cached version.
        if (ldr == U.gridClassLoader() && SYS_LDR_VER.get() != null)
            return SYS_LDR_VER.get();

        String usrVer = U.DFLT_USER_VERSION;

        InputStream in = ldr.getResourceAsStream(GRIDGAIN_XML_PATH);

        if (in != null) {
            // Note: use ByteArrayResource instead of InputStreamResource because
            // InputStreamResource doesn't work.
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try {
                U.copy(in, out);

                DefaultListableBeanFactory factory = new DefaultListableBeanFactory();

                XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(factory);

                reader.loadBeanDefinitions(new ByteArrayResource(out.toByteArray()));

                usrVer = (String)factory.getBean("userVersion");

                usrVer = usrVer == null ? U.DFLT_USER_VERSION : usrVer.trim();
            }
            catch (NoSuchBeanDefinitionException ignored) {
                if (log.isInfoEnabled())
                    log.info("User version is not explicitly defined (will use default version) [file=" +
                        GRIDGAIN_XML_PATH + ", clsLdr=" + ldr + ']');

                usrVer = U.DFLT_USER_VERSION;
            }
            catch (BeansException e) {
                U.error(log, "Failed to parse Spring XML file (will use default user version) [file=" +
                    GRIDGAIN_XML_PATH + ", clsLdr=" + ldr + ']', e);

                usrVer = U.DFLT_USER_VERSION;
            }
            catch (IOException e) {
                U.error(log, "Failed to read Spring XML file (will use default user version) [file=" +
                    GRIDGAIN_XML_PATH + ", clsLdr=" + ldr + ']', e);

                usrVer = U.DFLT_USER_VERSION;
            }
            finally {
                U.close(out, log);
            }
        }

        // For system class loader return cached version.
        if (ldr == U.gridClassLoader())
            SYS_LDR_VER.compareAndSet(null, usrVer);

        return usrVer;
    }

    /**
     * Gets bean configuration.
     *
     * @param ctx Spring context.
     * @param beanCls Bean class.
     * @return Spring bean.
     */
    @Nullable private static <T> T bean(ListableBeanFactory ctx, Class<T> beanCls) {
        Map.Entry<String, T> entry = F.firstEntry(ctx.getBeansOfType(beanCls));

        return entry == null ? null : entry.getValue();
    }

    /**
     * Creates Spring application context. Optionally excluded properties can be specified,
     * it means that if such a property is found in {@link org.apache.ignite.configuration.IgniteConfiguration}
     * then it is removed before the bean is instantiated.
     * For example, {@code streamerConfiguration} can be excluded from the configs that Visor uses.
     *
     * @param cfgUrl Resource where config file is located.
     * @param excludedProps Properties to be excluded.
     * @return Spring application context.
     */
    public static ApplicationContext applicationContext(URL cfgUrl, final String... excludedProps) {
        GenericApplicationContext springCtx = new GenericApplicationContext();

        BeanFactoryPostProcessor postProc = new BeanFactoryPostProcessor() {
            @Override public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory)
                throws BeansException {
                for (String beanName : beanFactory.getBeanDefinitionNames()) {
                    BeanDefinition def = beanFactory.getBeanDefinition(beanName);

                    if (def.getBeanClassName() != null) {
                        try {
                            Class.forName(def.getBeanClassName());
                        }
                        catch (ClassNotFoundException ignored) {
                            ((BeanDefinitionRegistry)beanFactory).removeBeanDefinition(beanName);

                            continue;
                        }
                    }

                    MutablePropertyValues vals = def.getPropertyValues();

                    for (PropertyValue val : new ArrayList<>(vals.getPropertyValueList())) {
                        for (String excludedProp : excludedProps) {
                            if (val.getName().equals(excludedProp))
                                vals.removePropertyValue(val);
                        }
                    }
                }
            }
        };

        springCtx.addBeanFactoryPostProcessor(postProc);

        new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(cfgUrl));

        springCtx.refresh();

        return springCtx;
    }
}
