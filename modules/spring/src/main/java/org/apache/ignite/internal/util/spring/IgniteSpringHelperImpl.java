/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.spring;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.resource.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
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
 * Spring configuration helper.
 */
public class IgniteSpringHelperImpl implements IgniteSpringHelper {
    /** Path to {@code ignite.xml} file. */
    public static final String IGNITE_XML_PATH = "META-INF/ignite.xml";

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
        URL cfgUrl, String... excludedProps) throws IgniteCheckedException {
        return loadConfigurations(cfgUrl, IgniteConfiguration.class, excludedProps);
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteBiTuple<Collection<T>, ? extends GridSpringResourceContext> loadConfigurations(
        URL cfgUrl, Class<T> cl, String... excludedProps) throws IgniteCheckedException {
        ApplicationContext springCtx = applicationContext(cfgUrl, excludedProps);
        Map<String, T> cfgMap;

        try {
            cfgMap = springCtx.getBeansOfType(cl);
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate bean [type=" + cl +
                ", err=" + e.getMessage() + ']', e);
        }

        if (cfgMap == null || cfgMap.isEmpty())
            throw new IgniteCheckedException("Failed to find configuration in: " + cfgUrl);

        return F.t(cfgMap.values(), new GridSpringResourceContextImpl(springCtx));
    }

    /** {@inheritDoc} */
    @Override public Map<Class<?>, Object> loadBeans(URL cfgUrl, Class<?>... beanClasses) throws IgniteCheckedException {
        assert beanClasses.length > 0;

        ApplicationContext springCtx = initContext(cfgUrl);

        Map<Class<?>, Object> beans = new HashMap<>();

        for (Class<?> cls : beanClasses)
            beans.put(cls, bean(springCtx, cls));

        return beans;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T loadBean(URL url, String beanName) throws IgniteCheckedException {
        ApplicationContext springCtx = initContext(url);

        try {
            return (T)springCtx.getBean(beanName);
        }
        catch (NoSuchBeanDefinitionException e) {
            throw new IgniteCheckedException("Spring bean with provided name doesn't exist [url=" + url +
                ", beanName=" + beanName + ']');
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to load Spring bean with provided name [url=" + url +
                ", beanName=" + beanName + ']', e);
        }
    }

    /**
     * @param url XML file URL.
     * @return Context.
     * @throws IgniteCheckedException In case of error.
     */
    private ApplicationContext initContext(URL url) throws IgniteCheckedException {
        GenericApplicationContext springCtx;

        try {
            springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(url));

            springCtx.refresh();
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + url + ']', e);
            else
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context [springUrl=" +
                    url + ", err=" + e.getMessage() + ']', e);
        }

        return springCtx;
    }

    /** {@inheritDoc} */
    @Override public String userVersion(ClassLoader ldr, IgniteLogger log) {
        assert ldr != null;
        assert log != null;

        // For system class loader return cached version.
        if (ldr == U.gridClassLoader() && SYS_LDR_VER.get() != null)
            return SYS_LDR_VER.get();

        String usrVer = U.DFLT_USER_VERSION;

        InputStream in = ldr.getResourceAsStream(IGNITE_XML_PATH);

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
                        IGNITE_XML_PATH + ", clsLdr=" + ldr + ']');

                usrVer = U.DFLT_USER_VERSION;
            }
            catch (BeansException e) {
                U.error(log, "Failed to parse Spring XML file (will use default user version) [file=" +
                    IGNITE_XML_PATH + ", clsLdr=" + ldr + ']', e);

                usrVer = U.DFLT_USER_VERSION;
            }
            catch (IOException e) {
                U.error(log, "Failed to read Spring XML file (will use default user version) [file=" +
                    IGNITE_XML_PATH + ", clsLdr=" + ldr + ']', e);

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
     * @throws IgniteCheckedException If configuration could not be read.
     */
    public static ApplicationContext applicationContext(URL cfgUrl, final String... excludedProps) throws IgniteCheckedException {
        try {
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
                                ((BeanDefinitionRegistry) beanFactory).removeBeanDefinition(beanName);

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
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) " +
                    "[springUrl=" + cfgUrl + ']', e);
            else
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context [springUrl=" +
                    cfgUrl + ", err=" + e.getMessage() + ']', e);
        }
    }
}
