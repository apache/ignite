package org.shaofan.s3.config;


import javax.servlet.MultipartConfigElement;

import org.shaofan.s3.intecept.S3Intecept;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
public class WebConfig implements WebMvcConfigurer {
    @Autowired
    private S3Intecept s3Intecept;
    
    @Autowired
    private SystemConfig config;
   
}
