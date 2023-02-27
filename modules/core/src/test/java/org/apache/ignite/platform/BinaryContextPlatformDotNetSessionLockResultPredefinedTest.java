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

package org.apache.ignite.platform;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryClassDescriptor;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.TestCachingMetadataHandler;
import org.apache.ignite.internal.processors.platform.websession.PlatformDotNetSessionLockResult;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for registering PlatformDotNetSessionLockResult class.
 */
public class BinaryContextPlatformDotNetSessionLockResultPredefinedTest {

    /** */
    @Test
    public void testBinaryContextRegisterPredefinedTypes() throws Exception {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setIgniteInstanceName("test");

        BinaryContext binCtx = new BinaryContext(new TestCachingMetadataHandler(), igniteCfg, null);

        BinaryClassDescriptor descriptorOne = binCtx.registerPredefinedType(PlatformDotNetSessionLockResult.class, 0);

        binCtx = new BinaryContext(new TestCachingMetadataHandler(), igniteCfg, null);

        BinaryClassDescriptor descriptorTwo = binCtx.registerPredefinedType(PlatformDotNetSessionLockResult.class, 0);

        Field f = BinaryClassDescriptor.class.getDeclaredField("ctor");
        f.setAccessible(true);

        Field constructorAccessor = Constructor.class.getDeclaredField("constructorAccessor");
        constructorAccessor.setAccessible(true);

        Constructor ctorOne = (Constructor) f.get(descriptorOne);

        Constructor ctorTwo = (Constructor) f.get(descriptorTwo);

        Object ctorOneAccessor = constructorAccessor.get(ctorOne);

        Object ctorTwoAccessor = constructorAccessor.get(ctorTwo);

        Assert.assertEquals(ctorOne, ctorTwo);
        Assert.assertNull(ctorOneAccessor);
        Assert.assertNull(ctorTwoAccessor);
    }
}
