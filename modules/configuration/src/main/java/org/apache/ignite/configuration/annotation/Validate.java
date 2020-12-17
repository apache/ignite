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

package org.apache.ignite.configuration.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.apache.ignite.configuration.validation.FieldValidator;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * This annotation applies custom validation to configuration field, for example:
 * <pre name="code" class="java">
 * public class ConfSchema {
 *     {@literal @}Validate(SomeCustomValidator.class)
 *     private String value;
 * }
 * </pre>
 *
 * If you need multiple custom validations:
 * <pre name="code" class="java">
 * public class ConfSchema {
 *     {@literal @}Validate(SomeCustomValidator.class),
 *     {@literal @}Validate(SomeOtherCustomValidator.class)
 *     private String value;
 * }
 * </pre>
 */
@Target({ FIELD })
@Retention(SOURCE)
@Repeatable(Validate.List.class)
@Documented
public @interface Validate {
    /**
     * @return Validation class that is going to be instantiated for the field validation.
     */
    Class<? extends FieldValidator<?, ?>> value();

    /**
     * @return Validation error message.
     */
    String message() default "";

    /**
     * Defines several {@link Validate} annotations on the same element.
     *
     * @see Validate
     */
    @Target({ FIELD })
    @Retention(SOURCE)
    @Documented
    @interface List {

        Validate[] value();
    }

}
