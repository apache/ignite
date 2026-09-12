

package org.apache.ignite.console.config;

import java.util.function.Predicate;

import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.annotation.AuthenticationPrincipal;

import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;



/**
 * The Spring Boot configuration of the REST API documentation tool Swagger.
 */
@Configuration
public class SwaggerConfiguration {
    @Bean
    public OpenAPI springShopOpenAPI() {
        return new OpenAPI()
          .info(new Info().title("Console application REST API")
          .description("Console application REST API application")
          .version("v0.0.1")
          .license(new License().name("Apache 2.0").url("http://springdoc.org")))
          .externalDocs(new ExternalDocumentation()
          .description("Ignite Console Wiki Documentation")
          .url("https://ignite.apache.org/docs"));
    }
	
	@Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
          .group("console-public")
          .packagesToScan("org.apache.ignite.console")
          .pathsToMatch("/api/**")
          .build();
    }
	
}
