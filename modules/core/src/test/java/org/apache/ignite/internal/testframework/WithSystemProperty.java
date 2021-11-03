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

package org.apache.ignite.internal.testframework;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Annotation that defines a scope with specific system property configured.<br>
 * <br>
 * Might be used on class level or on method level. Multiple annotations might be applied to the same class/method.<br>
 * <br>
 * In short, these two approaches are basically equivalent:<br>
 * <br>
 * Short:
 * <pre>{@code  @WithSystemProperty(key = "name", value = "val")
 *  public class SomeTest {
 *  }
 * }</pre>
 * Long:
 * <pre>{@code  public class SomeTest {
 *      private static Object oldVal;
 *
 *      @BeforeClass
 *      public static void beforeClass() {
 *          oldVal = System.getProperty("name");
 *
 *          System.setProperty("name", "val");
 *      }
 *
 *      @AfterClass
 *      public static void afterClass() {
 *          if (oldVal == null)
 *              System.clearProperty("name");
 *          else
 *              System.setProperty("name", oldVal);
 *      }
 *  }
 * }</pre>
 *
 * <p>Same applies to methods with the difference that annotation translates into something like {@link BeforeEach} and {@link AfterEach}.
 * <br><br>
 * <pre>{@code  public class SomeTest {
 *      @Test
 *      @WithSystemProperty(key = "name", value = "val")
 *      public void test() {
 *          // ...
 *      }
 *  }
 * }</pre>
 * is equivalent to:
 * <pre>{@code  public class SomeTest {
 *      @Test
 *      public void test() {
 *          Object oldVal = System.getProperty("name");
 *
 *          try {
 *              // ...
 *          }
 *          finally {
 *              if (oldVal == null)
 *                  System.clearProperty("name");
 *              else
 *                  System.setProperty("name", oldVal);
 *          }
 *      }
 *  }
 * }</pre>
 * For class level annotation it applies system properties for the whole class hierarchy (ignoring interfaces, there's no linearization
 * implemented). More specific classes have higher priority and set their properties last. It all starts with {@link Object} which, of
 * course, is not annotated.<br>
 * <br>
 * Test methods do not inherit their annotations from overridden methods of super class.<br>
 * <br>
 * If more than one annotation is presented on class/method then they will be applied in the same order as they appear in code. It is
 * achieved with the help of {@link Repeatable} feature of Java annotations - {@link SystemPropertiesList} is automatically generated in
 * such cases. For that reason it is not recommended using {@link SystemPropertiesList} directly.
 *
 * @see System#setProperty(String, String)
 * @see SystemPropertiesExtension
 * @see SystemPropertiesList
 */
@Repeatable(SystemPropertiesList.class)
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface WithSystemProperty {
    /** The name of the system property. */
    String key();

    /** The value of the system property. */
    String value();
}
