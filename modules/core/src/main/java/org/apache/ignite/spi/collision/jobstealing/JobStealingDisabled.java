/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.collision.jobstealing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation disables job stealing if corresponding feature is configured.
 * Add this annotation to the job class to disable stealing this kind of jobs
 * from nodes where they were mapped to.
 * <p>
 * Here is an example of how this annotation can be attached to a job class:
 * <pre name="code" class="java">
 * &#64;GridJobStealingDisabled
 * public class MyJob extends ComputeJobAdapter&lt;Object&gt; {
 *     public Serializable execute() throws IgniteCheckedException {
 *         // Job logic goes here.
 *         ...
 *     }
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface JobStealingDisabled {
    // No-op.
}