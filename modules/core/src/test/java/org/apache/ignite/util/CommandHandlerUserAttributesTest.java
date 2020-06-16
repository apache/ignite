package org.apache.ignite.util;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.ConnectionAndSslParameters;
import org.apache.ignite.ssl.SslContextFactory;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_INTERVAL;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandHandler.ATTR_SECURITY_USER_CERTIFICATE_PEM;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;
import static org.junit.Assert.assertNotNull;

/**
 * Checks userAttributes in GridClientConfiguration contains X509Certificate
 * when CommandHandler used with SSL parameters.
 */
public class CommandHandlerUserAttributesTest {
    /** */
    @Test
    public void testUserAttributesContainsX509Certificate() throws Exception {
        CommandHandler cmdHndlr = new CommandHandler();

        char[] chars = "123456".toCharArray();

        ConnectionAndSslParameters params = new ConnectionAndSslParameters(CommandList.STATE.command(),
            DFLT_HOST, DFLT_PORT, "test","test",
            DFLT_PING_INTERVAL, DFLT_PING_TIMEOUT,false, false,DFLT_SSL_PROTOCOL,"",
            SslContextFactory.DFLT_KEY_ALGORITHM, "src/test/resources/client.jks",chars,
            SslContextFactory.DFLT_STORE_TYPE,"src/test/resources/trust.jks", chars,
            SslContextFactory.DFLT_STORE_TYPE);

        GridClientConfiguration clientCfg = Whitebox.invokeMethod(cmdHndlr, "getClientConfiguration", params);

        String strPem = clientCfg.getUserAttributes().get(ATTR_SECURITY_USER_CERTIFICATE_PEM);

        assertNotNull(fromPEM(strPem));
    }

    /** */
    private static X509Certificate fromPEM(String strPem) throws CertificateException {
        byte[] decode = Base64.getDecoder().decode(strPem.getBytes());

        return (X509Certificate)CertificateFactory.getInstance("X.509")
            .generateCertificate(new ByteArrayInputStream(decode));
    }
}
