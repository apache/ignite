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

import java.util.Map;
import org.apache.ignite.console.notification.INotificationDescriptor;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Mail configuration.
 */
@Validated
@ConfigurationProperties("spring.mail")
public class MailPropertiesEx {
    /** Return address. */
    private String from;

    /** Default template path. */
    private String dfltTemplatePath;

    /** Templates path. */
    private Map<String, String> templates;

    /** Web console url. */
    @NotEmpty(message = "Please specify the web console url, for example: http://<your-host-name>:<port-if-needed>")
    private String webConsoleUrl;

    /**
     * @return Username alias.
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from Username alias.
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * @return Default template path.
     */
    public String getDefaultTemplatePath() {
        return dfltTemplatePath;
    }

    /**
     * @param dfltTemplatePath Default template path.
     */
    public void setDefaultTemplatePath(String dfltTemplatePath) {
        this.dfltTemplatePath = dfltTemplatePath;
    }

    /**
     * @param desc Notification type.
     */
    public String getTemplatePath(INotificationDescriptor desc) {
        return templates == null ? dfltTemplatePath : templates.getOrDefault(desc.toString(), dfltTemplatePath);
    }

    /**
     * @return Templates path.
     */
    public Map<String, String> getTemplates() {
        return templates;
    }

    /**
     * @param templates New templates path.
     */
    public void setTemplates(Map<String, String> templates) {
        this.templates = templates;
    }

    /**
     * @return Web console url.
     */
    public String getWebConsoleUrl() {
        return webConsoleUrl;
    }

    /**
     * @param webConsoleUrl Web console url.
     */
    public void setWebConsoleUrl(String webConsoleUrl) {
        this.webConsoleUrl = webConsoleUrl;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MailPropertiesEx.class, this);
    }
}
