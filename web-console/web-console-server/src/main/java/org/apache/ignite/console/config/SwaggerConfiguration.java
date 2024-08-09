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

package org.apache.ignite.console.config;

import java.util.function.Predicate;

import org.springdoc.core.GroupedOpenApi;
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
