package org.apache.ignite.internal.processors.rest.igfs.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.format.support.DefaultFormattingConversionService;

@Configuration
public class WebConfig {   
    
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
    
    /**
     * @return range bean for region (range request)
     */
    @Bean
    public RangeConverter rangeConverter() {
      return new RangeConverter();
    }
    
    @Bean  
    public ConversionService conversionService() {  
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();  
        // 注册自定义转换器  
        conversionService.addConverter(new RangeConverter());  
        return conversionService;  
    }
   
}
