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

package org.apache.ignite.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.context.WebApplicationContext;
import org.apache.ignite.console.common.Utils;

/**
 * Web console launcher.
 */
@SpringBootApplication(exclude = MongoAutoConfiguration.class)
@EnableScheduling
public class Application extends SpringBootServletInitializer{
    /**
     * @param args Args.
     */
    public static void main(String[] args) {
    	Utils.run(Application.class, "Web Console", args);
    }      
    
    @Override
    protected WebApplicationContext run(SpringApplication application) {
    	WebApplicationContext ctx  = (WebApplicationContext) Utils.run(Application.class, "Web Console","");    	
    	return ctx;
    }
    
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
    	
        return application.sources(Application.class);
    }
   
}
