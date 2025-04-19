/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.testframework.junits;

import org.apache.ignite.internal.util.CommonUtils;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/** */
public class SkipTestIfJdkNewerRule implements TestRule {
    /** */
    private static final String jdkVersion = CommonUtils.jdkVersion();

    /** {@inheritDoc} */
    @Override public Statement apply(Statement base, Description desc) {
        Statement res = base;

        SkipTestIfIsJdkNewer ann = desc.getAnnotation(SkipTestIfIsJdkNewer.class);

        if (ann == null)
            ann = desc.getTestClass().getAnnotation(SkipTestIfIsJdkNewer.class);

        Assume.assumeTrue("Skipping test", ann == null || CommonUtils.majorJavaVersion(jdkVersion) <= ann.value());

        return res;
    }
}
