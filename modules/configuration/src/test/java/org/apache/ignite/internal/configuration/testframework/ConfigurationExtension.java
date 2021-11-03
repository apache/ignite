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

package org.apache.ignite.internal.configuration.testframework;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.internalSchemaExtensions;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.polymorphicSchemaExtensions;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import java.lang.reflect.Field;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.DynamicConfigurationChanger;
import org.apache.ignite.internal.configuration.RootInnerNode;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.util.ConfigurationNotificationsUtil;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.commons.support.HierarchyTraversalMode;

/**
 * JUnit extension to inject configuration instances into test classes.
 *
 * @see InjectConfiguration
 */
public class ConfigurationExtension implements BeforeEachCallback, AfterEachCallback,
        BeforeAllCallback, AfterAllCallback, ParameterResolver {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(ConfigurationExtension.class);
    
    /** Key to store {@link ConfigurationAsmGenerator} in {@link ExtensionContext.Store}. */
    private static final Object CGEN_KEY = new Object();
    
    /** Key to store {@link ExecutorService} in {@link ExtensionContext.Store}. */
    private static final Object POOL_KEY = new Object();
    
    /** {@inheritDoc} */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).put(POOL_KEY, newSingleThreadExecutor());
    }
    
    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        ExecutorService pool = context.getStore(NAMESPACE).remove(POOL_KEY, ExecutorService.class);
        
        pool.shutdownNow();
    }
    
    /** {@inheritDoc} */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();
        
        context.getStore(NAMESPACE).put(CGEN_KEY, cgen);
        
        Object testInstance = context.getRequiredTestInstance();
        
        ExecutorService pool = context.getStore(NAMESPACE).get(POOL_KEY, ExecutorService.class);
        
        for (Field field : getMatchingFields(testInstance.getClass())) {
            field.setAccessible(true);
            
            InjectConfiguration annotation = field.getAnnotation(InjectConfiguration.class);
            
            field.set(testInstance, cfgValue(field.getType(), annotation, cgen, pool));
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        context.getStore(NAMESPACE).remove(CGEN_KEY);
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean supportsParameter(
            ParameterContext parameterContext, ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        return parameterContext.isAnnotated(InjectConfiguration.class)
                && supportType(parameterContext.getParameter().getType());
    }
    
    /** {@inheritDoc} */
    @Override
    public Object resolveParameter(
            ParameterContext parameterContext,
            ExtensionContext extensionContext
    ) throws ParameterResolutionException {
        Parameter parameter = parameterContext.getParameter();
        
        ConfigurationAsmGenerator cgen =
                extensionContext.getStore(NAMESPACE).get(CGEN_KEY, ConfigurationAsmGenerator.class);
        
        try {
            ExecutorService pool = extensionContext.getStore(NAMESPACE).get(POOL_KEY, ExecutorService.class);
            
            return cfgValue(parameter.getType(), parameter.getAnnotation(InjectConfiguration.class), cgen, pool);
        } catch (ClassNotFoundException classNotFoundException) {
            throw new ParameterResolutionException(
                    "Cannot find a configuration schema class that matches " + parameter.getType().getCanonicalName(),
                    classNotFoundException
            );
        }
    }
    
    /**
     * Instantiates a configuration instance for injection.
     *
     * @param type       Type of the field or parameter. Class name must end with {@code Configuration}.
     * @param annotation Annotation present on the field or parameter.
     * @param cgen       Runtime code generator associated with the extension instance.
     * @param pool       Single-threaded executor service to perform configuration changes.
     * @return Mock configuration instance.
     * @throws ClassNotFoundException If corresponding configuration schema class is not found.
     * @see #supportType(Class)
     */
    private static Object cfgValue(
            Class<?> type,
            InjectConfiguration annotation,
            ConfigurationAsmGenerator cgen,
            ExecutorService pool
    ) throws ClassNotFoundException {
        // Trying to find a schema class using configuration naming convention. This code won't work for inner Java
        // classes, extension is designed to mock actual configurations from public API to configure Ignite components.
        Class<?> schemaClass = Class.forName(type.getCanonicalName() + "Schema");
        
        cgen.compileRootSchema(
                schemaClass,
                internalSchemaExtensions(List.of(annotation.internalExtensions())),
                polymorphicSchemaExtensions(List.of(annotation.polymorphicExtensions()))
        );
        
        // RootKey must be mocked, there's no way to instantiate it using a public constructor.
        RootKey rootKey = mock(RootKey.class);
        
        when(rootKey.key()).thenReturn("mock");
        when(rootKey.type()).thenReturn(LOCAL);
        when(rootKey.schemaClass()).thenReturn(schemaClass);
        when(rootKey.internal()).thenReturn(false);
        
        SuperRoot superRoot = new SuperRoot(s -> new RootInnerNode(rootKey, cgen.instantiateNode(schemaClass)));
        
        ConfigObject hoconCfg = ConfigFactory.parseString(annotation.value()).root();
        
        HoconConverter.hoconSource(hoconCfg).descend(superRoot);
        
        ConfigurationUtil.addDefaults(superRoot);
        
        // Reference to the super root is required to make DynamicConfigurationChanger#change method atomic.
        var superRootRef = new AtomicReference<>(superRoot);
        
        // Reference that's required for notificator.
        var cfgRef = new AtomicReference<DynamicConfiguration<?, ?>>();
        
        cfgRef.set(cgen.instantiateCfg(rootKey, new DynamicConfigurationChanger() {
            private final AtomicInteger storageRev = new AtomicInteger();
            
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<Void> change(ConfigurationSource change) {
                return CompletableFuture.supplyAsync(() -> {
                    SuperRoot sr = superRootRef.get();
                    
                    SuperRoot copy = sr.copy();
                    
                    change.descend(copy);
                    
                    ConfigurationUtil.dropNulls(copy);
                    
                    if (superRootRef.compareAndSet(sr, copy)) {
                        List<CompletableFuture<?>> futures = new ArrayList<>();
                        
                        ConfigurationNotificationsUtil.notifyListeners(
                                sr.getRoot(rootKey),
                                copy.getRoot(rootKey),
                                (DynamicConfiguration<InnerNode, ?>) cfgRef.get(),
                                storageRev.incrementAndGet(),
                                futures
                        );
                        
                        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
                    }
                    
                    return change(change);
                }, pool).thenCompose(Function.identity());
            }
            
            /** {@inheritDoc} */
            @Override
            public InnerNode getRootNode(RootKey<?, ?> rk) {
                return superRootRef.get().getRoot(rk);
            }
            
            /** {@inheritDoc} */
            @Override
            public <T> T getLatest(List<String> path) {
                return ConfigurationUtil.find(path, superRootRef.get(), true);
            }
        }));
        
        ConfigurationNotificationsUtil.touch(cfgRef.get());
        
        return cfgRef.get();
    }
    
    /**
     * Looks for the annotated field inside the given test class.
     *
     * @return Annotated fields.
     */
    private static List<Field> getMatchingFields(Class<?> testClass) {
        return AnnotationSupport.findAnnotatedFields(
                testClass,
                InjectConfiguration.class,
                field -> supportType(field.getType()),
                HierarchyTraversalMode.TOP_DOWN
        );
    }
    
    /**
     * Checks that instance of the given class can be injected by the extension.
     *
     * @param type Field or parameter type.
     * @return {@code true} if value of the given class can be injected.
     */
    private static boolean supportType(Class<?> type) {
        return type.getCanonicalName().endsWith("Configuration");
    }
}
