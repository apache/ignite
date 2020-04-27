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

package org.apache.ignite.ml.dataset.primitive;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link DatasetWrapper}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DatasetWrapperTest {
    /** Mocked dataset. */
    @Mock
    private Dataset<Serializable, AutoCloseable> dataset;

    /** Dataset wrapper. */
    private DatasetWrapper<Serializable, AutoCloseable> wrapper;

    /** Initialization. */
    @Before
    public void beforeTest() {
        wrapper = new DatasetWrapper<>(dataset);
    }

    /** Tests {@code computeWithCtx()} method. */
    @Test
    @SuppressWarnings("unchecked")
    public void testComputeWithCtx() {
        doReturn(42).when(dataset).computeWithCtx(any(IgniteTriFunction.class), any(), any());

        //NOTE: don't remove this cast due to Java9+ compilation failure on different Java versions and OS.
        Integer res = (Integer)wrapper.computeWithCtx(mock(IgniteTriFunction.class), mock(IgniteBinaryOperator.class),
            null);

        assertEquals(42, res.intValue());

        verify(dataset, times(1)).computeWithCtx(any(IgniteTriFunction.class), any(), any());
    }

    /** Tests {@code computeWithCtx()} method. */
    @Test
    @SuppressWarnings("unchecked")
    public void testComputeWithCtx2() {
        doReturn(42).when(dataset).computeWithCtx(any(IgniteTriFunction.class), any(), any());

        //NOTE: don't remove this cast due to Java9+ compilation failure on different Java versions and OS.
        Integer res = (Integer)wrapper.computeWithCtx(mock(IgniteBiFunction.class), mock(IgniteBinaryOperator.class),
            null);

        assertEquals(42, res.intValue());

        verify(dataset, times(1)).computeWithCtx(any(IgniteTriFunction.class), any(), any());
    }

    /** Tests {@code computeWithCtx()} method. */
    @Test
    @SuppressWarnings("unchecked")
    public void testComputeWithCtx3() {
        wrapper.computeWithCtx((ctx, data) -> {
            assertNotNull(ctx);
            assertNotNull(data);
        });

        verify(dataset, times(1)).computeWithCtx(any(IgniteTriFunction.class),
            any(IgniteBinaryOperator.class), any());
    }

    /** Tests {@code compute()} method. */
    @Test
    @SuppressWarnings("unchecked")
    public void testCompute() {
        doReturn(42).when(dataset).compute(any(IgniteBiFunction.class), any(), any());

        //NOTE: don't remove this cast due to Java9+ compilation failure on different Java versions and OS.
        Integer res = (Integer)wrapper.compute(mock(IgniteBiFunction.class), mock(IgniteBinaryOperator.class),
            null);

        assertEquals(42, res.intValue());

        verify(dataset, times(1)).compute(any(IgniteBiFunction.class), any(), any());
    }

    /** Tests {@code compute()} method. */
    @Test
    @SuppressWarnings("unchecked")
    public void testCompute2() {
        doReturn(42).when(dataset).compute(any(IgniteBiFunction.class), any(IgniteBinaryOperator.class), any());

        //NOTE: don't remove this cast due to Java9+ compilation failure on different Java versions and OS.
        Integer res = (Integer)wrapper.compute(mock(IgniteFunction.class), mock(IgniteBinaryOperator.class),
            null);

        assertEquals(42, res.intValue());

        verify(dataset, times(1)).compute(any(IgniteBiFunction.class), any(IgniteBinaryOperator.class), any());
    }

    /** Tests {@code close()} method. */
    @Test
    public void testClose() throws Exception {
        wrapper.close();

        verify(dataset, times(1)).close();
    }
}
