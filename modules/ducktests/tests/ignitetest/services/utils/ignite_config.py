# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module renders Ignite config and all related artifacts
"""


class IgniteConfig:
    def __init__(self, project="ignite"):
        self.project = project

    def render(self, config_dir, work_dir, properties=""):
        return """<?xml version="1.0" encoding="UTF-8"?>

<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
                            http://www.springframework.org/schema/beans/spring-beans.xsd">
    <bean class="org.apache.ignite.configuration.IgniteConfiguration">
        <property name="workDirectory" value="{work_dir}" />
        <property name="gridLogger">
            <bean class="org.apache.ignite.logger.log4j.Log4JLogger">
                <constructor-arg type="java.lang.String" value="{config_dir}/ignite-log4j.xml"/>
            </bean>
        </property>
        {properties}
    </bean>
</beans>
        """.format(config_dir=config_dir,
                   work_dir=work_dir,
                   properties=properties)

    def render_log4j(self, work_dir):
        return """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE log4j:configuration PUBLIC "-//APACHE//DTD LOG4J 1.2//EN"
    "http://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/xml/doc-files/log4j.dtd">

<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/" debug="false">
    <appender name="CONSOLE_ERR" class="org.apache.log4j.ConsoleAppender">
        <param name="Target" value="System.err"/>

        <param name="Threshold" value="INFO"/>

        <layout class="org.apache.log4j.PatternLayout">
            <param name="ConversionPattern" value="[%d{{ISO8601}}][%-5p][%t][%c{{1}}] %m%n"/>
        </layout>
    </appender>

    <category name="org.springframework">
        <level value="WARN"/>
    </category>

    <category name="org.eclipse.jetty">
        <level value="WARN"/>
    </category>

    <category name="org.eclipse.jetty.util.log">
        <level value="ERROR"/>
    </category>

    <category name="org.eclipse.jetty.util.component">
        <level value="ERROR"/>
    </category>

    <category name="com.amazonaws">
        <level value="WARN"/>
    </category>

    <root>
        <level value="INFO"/>
        <appender-ref ref="CONSOLE_ERR"/>
    </root>
</log4j:configuration>
                """.format(work_dir=work_dir)
