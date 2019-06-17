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

import java.util.Locale;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.MessageSourceAccessor;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;

/**
 * Message source configuration.
 */
@Configuration
public class MessageSourceConfiguration {
    /**
     * Notification message source.
     */
    @Bean(name = "messageSource")
    @ConditionalOnMissingBean(name = "messageSource")
    public MessageSource notificationMessages() {
        ReloadableResourceBundleMessageSource msgSrc = new ReloadableResourceBundleMessageSource();

        msgSrc.setBasenames("classpath:i18/notifications");
        msgSrc.setDefaultEncoding("UTF-8");

        return msgSrc;
    }

    /**
     * @param msgSrc Message source.
     */
    @Bean
    public MessageSourceAccessor getMessageSourceAccessor(MessageSource msgSrc) {
        return new MessageSourceAccessor(msgSrc, Locale.US);
    }
}
