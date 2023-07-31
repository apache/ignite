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

package org.apache.ignite.internal.util.typedef.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Defines internal {@code typedef} for {@link IgniteUtils}. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended for internal
 * use only and meant to provide for more terse code when readability of code is not compromised.
 */
public class U extends IgniteUtils {
    public static final boolean FIX = true;

    public static final boolean TEST = true;

    public static final boolean TEST_DEBUG = TEST && false;
    public static final AtomicBoolean TEST_ACTION1 = new AtomicBoolean(true);
    public static final AtomicBoolean TEST_ACTION2 = new AtomicBoolean(true);
    public static final AtomicBoolean TEST_ACTION3 = new AtomicBoolean(true);
    public static final AtomicBoolean TEST_ACTION4 = new AtomicBoolean(true);
}
