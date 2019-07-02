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

package org.apache.ignite.testframework.junits;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.METHOD;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Repeat test N times annotation.
 * <p>Usage example:</p>
 * <pre>
 * <code>{@literal @}Test
 * {@literal @}Repeat(10)
 *  public void testSomething() {
 *     // test method body
 *  }
 * </code>
 * </pre>
 */
@Retention( RetentionPolicy.RUNTIME )
@Target({ METHOD, ANNOTATION_TYPE })
public @interface Repeat {
    /** */
    int value() default 1;
}
