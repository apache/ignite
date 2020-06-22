package org.apache.ignite.springdata.misc;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.spel.spi.EvaluationContextExtension;

/**
 * Sample EvaluationContext Extension for Spring Data 2.0
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

    private static final SamplePassParamExtension SAMPLE_PASS_PARAM_EXTENSION_INSTANCE = new SamplePassParamExtension();
    private static final Map<String, Object> properties = new HashMap<>();
    private static final String SAMPLE_EXTENSION_SPEL_VAR = "sampleExtension";

    static {
        properties.put(SAMPLE_EXTENSION_SPEL_VAR, SAMPLE_PASS_PARAM_EXTENSION_INSTANCE);
    }

    @Override
    public String getExtensionId() {
        return "HK-SAMPLE-PASS-PARAM-EXTENSION";
    }

    @Override
    public Map<String, Object> getProperties() {
        return properties;
    }

    public static class SamplePassParamExtension  {
        // just return same param
        public Object transformParam(Object param){
            return param;
        }
    }

}
