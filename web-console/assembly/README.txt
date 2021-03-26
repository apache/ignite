Requirements
-------------------------------------
1. JDK 8 suitable for your platform.
2. Supported browsers: Chrome, Firefox, Safari, Edge.
3. GridGain cluster should be started with `ignite-rest-http` module in classpath.
   For this copy `ignite-rest-http` folder from `libs\optional` to `libs` folder.

How to run
-------------------------------------
1. Unpack ignite-web-console-x.x.x.zip to some folder.
2. Change work directory to folder where Web Console was unpacked.
3. Start web-console.{bat|sh} executable for you platform.

4. Open URL `localhost:3000` in browser.
5. Sign up user.
6. Download Web Console agent.
7. Start Web Console agent from folder `ignite-web-console-agent-x.x.x`. For Web Agent settings see `ignite-web-console-agent-x.x.x\README.txt`.

NOTE: Cluster URL should be specified in `ignite-web-console-agent-x.x.x\default.properties` in `node-uri` parameter.

Technical details
-------------------------------------
1. Package content:
    `libs` - this folder contains Web Console dependencies.
    `work` - this folder contains all Web Console data (registered users, created objects, ...) and
     should be preserved in case of update to new version.
    `agent_dists` - this folder contains Web Agent.
2. Web console will start on default HTTP port `3000` and bind to all interfaces `0.0.0.0`.


You can use properties files, YAML files, environment variables to externalize configuration.

Properties are considered in the following order:
1. Java System properties (System.getProperties()).
2. OS environment variables.
3. Properties in /config subdirectory of the work directory or in work directory (application.properties and YAML variants).
   YAML format description can be found here https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html#boot-features-external-config-yaml


Various properties can be specified inside your application.properties file, inside your application.yml file, or Java System properties.
This appendix provides a list of common Spring Boot properties and references to the underlying classes that consume them:

# Sign up configuration.
account.signup.enabled=true # Enable self registration
account.activation.enabled=false # Enable account activation
account.activation.timeout=1800000 # Activation timeout(milliseconds)
account.activation.sendTimeout=180000 # Activation send email throttle (milliseconds)

# Embedded server configuration.
server.address=0.0.0.0 # Network address to which the Web Console should bind.
server.port=3000 # Web Console HTTP port.

# Email (MailProperties)
spring.mail.default-encoding=UTF-8 # Default MimeMessage encoding.
spring.mail.host= # SMTP server host. For instance, `smtp.example.com`.
spring.mail.jndi-name= # Session JNDI name. When set, takes precedence over other Session settings.
spring.mail.password= # Login password of the SMTP server.
spring.mail.port= # SMTP server port.
spring.mail.properties.*= # Additional JavaMail Session properties.
spring.mail.protocol=smtp # Protocol used by the SMTP server.
spring.mail.test-connection=false # Whether to test that the mail server is available on startup.
spring.mail.username= # Login user of the SMTP server.
spring.mail.web-console-url= # Web console url for links in notifications.

# SSL options.
server.ssl.ciphers= # Supported SSL ciphers.
server.ssl.client-auth= # Client authentication mode.
server.ssl.enabled=true # Whether to enable SSL support.
server.ssl.enabled-protocols= # Enabled SSL protocols.
server.ssl.key-alias= # Alias that identifies the key in the key store.
server.ssl.key-password= # Password used to access the key in the key store.
server.ssl.key-store= # Path to the key store that holds the SSL certificate (typically a jks file).
server.ssl.key-store-password= # Password used to access the key store.
server.ssl.key-store-provider= # Provider for the key store.
server.ssl.key-store-type= # Type of the key store.
server.ssl.protocol=TLS # SSL protocol to use.
server.ssl.trust-store= # Trust store that holds SSL certificates.
server.ssl.trust-store-password= # Password used to access the trust store.
server.ssl.trust-store-provider= # Provider for the trust store.
server.ssl.trust-store-type= # Type of the trust store.


Mail setup
-------------------------------------

For example, the properties for Gmail SMTP Server can be specified as:
spring.mail.host=smtp.gmail.com
spring.mail.port=587
spring.mail.username=<login user to smtp server>
spring.mail.password=<login password to smtp server>
spring.mail.web-console-url=http://<your-web-console-host>:<port-if-needed>
spring.mail.properties.mail.smtp.auth=true
spring.mail.properties.mail.smtp.starttls.enable=true


HTTPS setup (with self-signed certificate) between Web Agent and Web Console
-------------------------------------
1. Generate self-signed certificate.
2. Configure Web Console to use SSL port: 443 and key-store with generated certificate.
3. Configure Web Agent to use same certificate (use options "-sks path-to-key-store -sksp key-store-password" OR set server-key-store and server-key-store-password in default.properties).

For example, the properties for Web Console can be specified as:
server.port=443
server.ssl.key-store-type=JKS
server.ssl.key-store=certificates/server.jks
server.ssl.key-store-password=change_me

HTTPS setup (with self-signed certificate) between Web Agent and Cluster
-------------------------------------
1. Generate self-signed certificate and trust store.
2. Prepare configuration for Jetty web server with HTTPS support.
3. Prepare configuration for cluster that will refer to Jetty configuration from step 2.
4. Start Web Agent with "-Dtrust.all=true" option (because of self-signed certificate).

Following Jetty configuration should be created to enable ssl/tls connect
(more details about Jetty configuration can be found https://wiki.eclipse.org/Jetty/Howto/Configure_SSL):
<?xml version="1.0"?>

<!DOCTYPE Configure PUBLIC "-//Jetty//Configure//EN" "http://www.eclipse.org/jetty/configure.dtd">

<Configure id="Server" class="org.eclipse.jetty.server.Server">
    <Arg name="threadPool">
        <!-- Default queued blocking thread pool -->
        <New class="org.eclipse.jetty.util.thread.QueuedThreadPool">
            <Set name="minThreads">10</Set>
            <Set name="maxThreads">200</Set>
        </New>
    </Arg>

    <New id="httpsCfg" class="org.eclipse.jetty.server.HttpConfiguration">
        <Set name="secureScheme">https</Set>
        <Set name="securePort"><SystemProperty name="IGNITE_JETTY_PORT" default="8443"/></Set>
        <Set name="sendServerVersion">true</Set>
        <Set name="sendDateHeader">true</Set>
        <Call name="addCustomizer">
            <Arg><New class="org.eclipse.jetty.server.SecureRequestCustomizer"/></Arg>
        </Call>
    </New>

    <New id="sslContextFactory" class="org.eclipse.jetty.util.ssl.SslContextFactory">
        <Set name="keyStorePath">/opt/keystore/server.jks</Set>
        <Set name="keyStorePassword">123456</Set>
        <Set name="keyManagerPassword">123456</Set>
        <Set name="trustStorePath">/opt/keystore/trust.jks</Set>
        <Set name="trustStorePassword">123456</Set>
    </New>

    <Call name="addConnector">
        <Arg>
            <New class="org.eclipse.jetty.server.ServerConnector">
                <Arg><Ref refid="Server"/></Arg>
                <Arg>
                    <Array type="org.eclipse.jetty.server.ConnectionFactory">
                        <Item>
                            <New class="org.eclipse.jetty.server.SslConnectionFactory">
                                <Arg><Ref refid="sslContextFactory"/></Arg>
                                <Arg>http/1.1</Arg>
                            </New>
                        </Item>
                        <Item>
                            <New class="org.eclipse.jetty.server.HttpConnectionFactory">
                                <Arg><Ref refid="httpsCfg"/></Arg>
                            </New>
                        </Item>
                    </Array>
                </Arg>
                <!--
                    Note that in order to override local host and port values,
                    system properties must have names IGNITE_JETTY_HOST and
                    IGNITE_JETTY_PORT accordingly.
                -->
                <Set name="host"><SystemProperty name="IGNITE_JETTY_HOST" default="0.0.0.0"/></Set>
                <Set name="port"><SystemProperty name="IGNITE_JETTY_PORT" default="8443"/></Set>
                <Set name="idleTimeout">30000</Set>
                <Set name="reuseAddress">true</Set>
            </New>
        </Arg>
    </Call>

    <Set name="handler">
        <New id="Handlers" class="org.eclipse.jetty.server.handler.HandlerCollection">
            <Set name="handlers">
                <Array type="org.eclipse.jetty.server.Handler">
                    <Item>
                        <New id="Contexts" class="org.eclipse.jetty.server.handler.ContextHandlerCollection"/>
                    </Item>
                </Array>
            </Set>
        </New>
    </Set>

    <Set name="stopAtShutdown">false</Set>
</Configure>

Path to this configuration should be pass to ConnectorConfiguration.setJettyPath(String) property in node configuration.
Path to Jetty configuration file should be either absolute or relative to IGNITE_HOME.
(more details about REST HTTP configuration can be found https://apacheignite.readme.io/docs/rest-api#section-general-configuration)

Jetty configuration and node configuration should be same for all nodes.
If Jetty configuration is not provided for some nodes then Ignite(GridGain) will start Jetty server with simple HTTP connector.

Example of grid configuration:
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="
        http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">

    <import resource="common.xml"/>

    <bean id="grid.cfg" class="org.apache.ignite.configuration.IgniteConfiguration">
        <!-- Enable SSL for REST HTTP -->
        <property name="connectorConfiguration">
            <bean class="org.apache.ignite.configuration.ConnectorConfiguration">
                <property name="jettyPath" value="<path to rest-jetty-ssl.xml>"/>
            </bean>
        </property>
    </bean>
</beans>


Migration from previous version of Web Console (based on Mongo DB).
-------------------------------------
1. Configure MongDB database name for migration:
migration.mongo.db.url: mongodb://localhost:27017/console

2. Start Mongo DB on the same host with Web Console backend.

3. Start Web Console backed, data will be migrated during startup.

4. It is recommended to remove "migration.mongo.db.url" from settings after migration.

5. Migration possible only on "clean" GridGain database.
 If you need to repeat migration, just delete folder "work" and restart Web Console.
