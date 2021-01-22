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

package org.apache.ignite.configuration.sample;

import javax.validation.constraints.Min;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;

/**
 * Test cache configuration schema.
 */
@Config
public class CacheConfigurationSchema {
    /** Size. */
    @Value
    @Min(value = 1, message = "Minimal cache size is 1")
    private int size;

}
