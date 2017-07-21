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

package org.apache.ignite.internal.processors.resource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * Unit tests for grid resource processor.
 */
@GridCommonTest(group = "Resource Self")
public class GridResourceProcessorSelfTest extends GridCommonAbstractTest {
    /** */
    private GridTestKernalContext ctx;

    /** */
    public GridResourceProcessorSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.add(new GridResourceProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx.stop(true);
    }

    /** */
    @Target({ElementType.METHOD, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    private static @interface TestAnnotation {
        // No-op.
    }

    /** */
    @Target({ElementType.METHOD, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    private static @interface TestAnnotation2 {
        // No-op.
    }

    /** */
    @Target({ElementType.METHOD, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    private static @interface TestAnnotation3 {
        // No-op.
    }

    /** */
    private static class TestClassWithAnnotatedField {
        /** */
        @TestAnnotation
        private String str;

        /**
         * @return Value of the field.
         */
        public String getStr() {
            return str;
        }

        /**
         * @param str New value.
         */
        public void setStr(String str) {
            this.str = str;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectResourceToAnnotatedField() throws Exception {
        TestClassWithAnnotatedField target = new TestClassWithAnnotatedField();

        String testStr = Long.toString(System.currentTimeMillis());

        ctx.resource().injectBasicResource(target, TestAnnotation.class, testStr);

        assertEquals(testStr, target.str);

        ctx.resource().injectBasicResource(target, TestAnnotation2.class, "Some another string.");

        // Value should not be updated.
        assertEquals(testStr, target.str);
    }

    /** */
    private static class TestClassWithAnnotatedMethod {
        /** */
        private String str;

        /**
         * @param str New value of the field.
         */
        @TestAnnotation
        void setStr(String str) {
            this.str = str;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectResourceToAnnotatedMethod() throws Exception {
        TestClassWithAnnotatedMethod target = new TestClassWithAnnotatedMethod();

        String testStr = Long.toString(System.currentTimeMillis());

        ctx.resource().injectBasicResource(target, TestAnnotation.class, testStr);

        assertEquals(testStr, target.str);

        ctx.resource().injectBasicResource(target, TestAnnotation2.class, "Some another string.");

        // Value should not be updated.
        assertEquals(testStr, target.str);
    }

    /** */
    private static class TestClassWithAnnotationsOuter {
        /** */
        @TestAnnotation
        private String str1;

        /** */
        @TestAnnotation
        private String str2;

        /** */
        @SuppressWarnings({"UnusedDeclaration"}) @TestAnnotation3
        private String str7;

        /**
         * @param str1 New value.
         */
        @TestAnnotation2
        public void setValue1(String str1) {
            this.str1 = str1;
        }

        /**
         * @param str2 New value.
         */
        @TestAnnotation2
        public void setValue2(String str2) {
            this.str2 = str2;
        }

        /** */
        private class TestClassWithAnnotationsInner {
            /** */
            @TestAnnotation
            private String str3;

            /** */
            @TestAnnotation
            private String str4;

            /**
             * @param str3 New value.
             */
            @TestAnnotation2
            public void setValue3(String str3) { this.str3 = str3; }

            /**
             * @param str4 New value.
             */
            @TestAnnotation2
            public void setValue4(String str4) { this.str4 = str4; }

            /** */
            private class TestClassWithAnnotationsDeep {
                /** */
                @TestAnnotation
                private String str5;

                /** */
                @TestAnnotation
                private String str6;

                private Callable<String> c = new Callable<String>() {
                    @SuppressWarnings({"UnusedDeclaration"}) @TestAnnotation
                    private String cStr;

                    private Runnable r = new Runnable() {
                        @SuppressWarnings({"UnusedDeclaration"}) @TestAnnotation
                        private String rStr;

                        @Override public void run() {
                            assert cStr != null;

                            assertEquals(cStr, rStr);
                        }
                    };

                    @Override public String call() throws Exception {
                        assert str5 != null;

                        assertEquals(str5, cStr);

                        r.run();

                        return cStr;
                    }
                };

                /**
                 * @param str5 New value.
                 */
                @TestAnnotation2
                public void setValue5(String str5) { this.str5 = str5; }

                /**
                 * @param str6 New value.
                 */
                @TestAnnotation2
                public void setValue6(String str6) { this.str6 = str6; }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectResourceInnerClasses() throws Exception {
        // Test fields.
        TestClassWithAnnotationsOuter outer = new TestClassWithAnnotationsOuter();

        TestClassWithAnnotationsOuter.TestClassWithAnnotationsInner inner = outer.new TestClassWithAnnotationsInner();

        TestClassWithAnnotationsOuter.TestClassWithAnnotationsInner.TestClassWithAnnotationsDeep deep = inner.new TestClassWithAnnotationsDeep();

        String testStr = Long.toString(System.currentTimeMillis());

        ctx.resource().injectBasicResource(deep, TestAnnotation.class, testStr);

        assertEquals(testStr, outer.str1);
        assertEquals(testStr, outer.str2);
        assertEquals(testStr, inner.str3);
        assertEquals(testStr, inner.str4);
        assertEquals(testStr, deep.str5);
        assertEquals(testStr, deep.str6);

        // Check if all resources have been injected to nested callable and runnable.
        deep.c.call();

        // Test methods.
        outer = new TestClassWithAnnotationsOuter();

        inner = outer.new TestClassWithAnnotationsInner();

        deep = inner.new TestClassWithAnnotationsDeep();

        ctx.resource().injectBasicResource(deep, TestAnnotation2.class, testStr);

        assertEquals(testStr, outer.str1);
        assertEquals(testStr, outer.str2);
        assertEquals(testStr, inner.str3);
        assertEquals(testStr, inner.str4);
        assertEquals(testStr, deep.str5);
        assertEquals(testStr, deep.str6);

        assertNull(outer.str7);

        ctx.resource().injectBasicResource(deep, TestAnnotation3.class, testStr);

        assertEquals(testStr, outer.str7);
    }

    /**
     * Test task.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger taskLog;

        /**
         * @return Task resource.
         */
        public IgniteLogger getTaskLog() {
            return taskLog;
        }

        /**
         * Creates a single job.
         *
         * @param gridSize Grid size.
         * @param arg Task argument.
         */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            assert taskLog != null;

            final IgniteOutClosure<Object> callable = new IgniteOutClosure<Object>() {
                /** Should be injected despite this is a {@link Callable} instance nested in a job. */
                @IgniteInstanceResource
                private Ignite grid;

                /** Runnable object nested inside callable. */
                private Runnable run = new IgniteRunnable() {
                    @IgniteInstanceResource
                    private Ignite ignite;

                    @Override public void run() {
                        assert ignite != null;
                        assert ignite.configuration() != null;
                        assert ignite.configuration().getIgniteHome() != null;
                    }
                };

                @Override public Object apply() {
                    assert grid != null;

                    run.run();

                    return new Object();
                }
            };

            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return callable.apply();
                }
            });
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectResourceGridTaskAndJob() throws Exception {
        Ignite g = startGrid();

        try {
            // Should not be null if task has been completed successfully (meaning all resources have been injected).
            Assert.notNull(g.compute().execute(TestTask.class, null));
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjectResourcePerformance() throws Exception {
        int injNum = 50000;

        long start = System.currentTimeMillis();

        TestClassWithAnnotatedField target = new TestClassWithAnnotatedField();

        for (int i = 0; i < injNum; i++)
            ctx.resource().injectBasicResource(target, TestAnnotation.class, "Test string.");

        long duration = System.currentTimeMillis() - start;

        info("Resource injection takes " + ((double)duration / injNum) + " msec per target object.");
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public void testInjectResourceMultiThreaded() throws Exception {
        final int threadsCnt = 100;
        final int iters = 2000000;

        ctx = newContext();

        ctx.add(new GridResourceProcessor(ctx));

        ctx.start();

        try {
            GridTestUtils.runMultiThreaded(new Runnable() {
                @Override public void run() {
                    try {
                        Test1 obj = new Test1();

                        long start = System.currentTimeMillis();

                        for (int i = 0; i < iters; i++)
                            ctx.resource().injectBasicResource(obj, TestAnnotation1.class, "value");

                        long duration = (System.currentTimeMillis() - start);

                        float avgInjectTime = Math.round(1000.0f * duration / iters) / 1000.0f;

                        info("Finished load test [avgInjectTime=" + avgInjectTime +
                            "ms, duration=" + duration + "ms, count=" + iters + ']');
                    }
                    catch (IgniteCheckedException e) {
                        fail("Failed to inject resources: " + e.getMessage());
                    }
                }
            }, threadsCnt, "grid-ioc-test");
        }
        finally {
            ctx.stop(true);
        }
    }

    /**
     *
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.FIELD})
    private @interface TestAnnotation1 {
        // No-op.
    }

    /**
     *
     */
    private static final class Test1 {
        /** */
        @SuppressWarnings({"unused", "UnusedDeclaration"})
        @TestAnnotation1
        private String val1;
    }
}