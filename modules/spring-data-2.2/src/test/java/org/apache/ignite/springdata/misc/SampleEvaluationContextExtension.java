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
package org.apache.ignite.springdata.misc;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.spel.spi.EvaluationContextExtension;

/**
 * Sample EvaluationContext Extension for Spring Data 2.2
 * <p>
 * Use SpEl expressions into your {@code @Query} definitions.
 * <p>
 * First, you need to register your extension into your spring data configuration. Sample:
 * <pre>
 * {@code @Configuration}
 * {@code @EnableIgniteRepositories}(basePackages = ... )
 * public class MyIgniteRepoConfig {
 * ...
 *      {@code @Bean}
 *      public EvaluationContextExtension sampleSpELExtension() {
 *          return new SampleEvaluationContextExtension();
 *      }
 * ...
 * }
 * </pre>
 *
 * <p>
 * Sample of usage into your {@code @Query} definitions:
 * <pre>
 * {@code @RepositoryConfig}(cacheName = "users")
 * public interface UserRepository
 * extends IgniteRepository<User, UUID>{
 *     [...]
 *
 *     {@code @Query}(value = "SELECT * from #{#entityName} where email = ?#{sampleExtension.transformParam(#email)}")
 *     User searchUserByEmail(@Param("email") String email);

 *      [...]
 *     }
 * </pre>
 * <p>
 *
 * @author Manuel Núñez Sánchez (manuel.nunez@hawkore.com)
 */
public class SampleEvaluationContextExtension implements EvaluationContextExtension {
    /** */
    private static final SamplePassParamExtension SAMPLE_PASS_PARAM_EXTENSION_INSTANCE = new SamplePassParamExtension();

    /** */
    private static final Map<String, Object> properties = new HashMap<>();

    /** */
    private static final String SAMPLE_EXTENSION_SPEL_VAR = "sampleExtension";

    static {
        properties.put(SAMPLE_EXTENSION_SPEL_VAR, SAMPLE_PASS_PARAM_EXTENSION_INSTANCE);
    }

    /** */
    @Override public String getExtensionId() {
        return "HK-SAMPLE-PASS-PARAM-EXTENSION";
    }

    /** */
    @Override public Map<String, Object> getProperties() {
        return properties;
    }

    /** */
    public static class SamplePassParamExtension {
        // just return same param
        /** */
        public Object transformParam(Object param) {
            return param;
        }
    }
}
