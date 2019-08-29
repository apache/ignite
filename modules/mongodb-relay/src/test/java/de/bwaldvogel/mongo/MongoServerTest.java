package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

public abstract class MongoServerTest {

    private static final int TEST_TIMEOUT_MILLIS = 10_000;

    private MongoServer server;

    protected abstract MongoBackend createBackend() throws Exception;

    @Before
    public void setUp() throws Exception {
        server = new MongoServer(createBackend());
    }

    @After
    public void tearDown() {
        server.shutdown();
    }

    @Test
    public void testToString() throws Exception {
        assertThat(server).hasToString("MongoServer()");
        InetSocketAddress inetSocketAddress = server.bind();
        assertThat(server).hasToString("MongoServer(port: " + inetSocketAddress.getPort() + ", ssl: false)");
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testStopListening() throws Exception {
        InetSocketAddress serverAddress = server.bind();
        try (MongoClient client = new MongoClient(new ServerAddress(serverAddress))) {
            // request something
            pingServer(client);

            server.stopListenting();

            // existing clients must still work
            pingServer(client);

            // new clients must fail
            assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> {
                    try (Socket socket = new Socket()) {
                        socket.connect(serverAddress);
                    }
                });
        }
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testShutdownNow() throws Exception {
        InetSocketAddress serverAddress = server.bind();
        MongoClient client = new MongoClient(new ServerAddress(serverAddress));

        // request something to open a connection
        pingServer(client);

        server.shutdownNow();
    }

    @Test(timeout = 5000)
    public void testGetLocalAddress() throws Exception {
        assertThat(server.getLocalAddress()).isNull();
        InetSocketAddress serverAddress = server.bind();
        InetSocketAddress localAddress = server.getLocalAddress();
        assertThat(localAddress).isEqualTo(serverAddress);
        server.shutdownNow();
        assertThat(server.getLocalAddress()).isNull();
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testShutdownAndRestart() throws Exception {
        InetSocketAddress serverAddress = server.bind();
        try (MongoClient client = new MongoClient(new ServerAddress(serverAddress))) {
            // request something to open a connection
            pingServer(client);

            server.shutdownNow();

            assertThatExceptionOfType(MongoException.class)
                .isThrownBy(() -> pingServer(client));

            // restart
            server.bind(serverAddress);

            pingServer(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_MILLIS)
    public void testSsl() throws Exception {
        server.enableSsl(getPrivateKey(), null, getCertificate());
        InetSocketAddress serverAddress = server.bind();
        assertThat(server).hasToString("MongoServer(port: " + serverAddress.getPort() + ", ssl: true)");

        MongoClientOptions clientOptions = MongoClientOptions.builder()
            .sslEnabled(true)
            //-.sslContext(createSslContext(loadTestKeyStore()))
            .build();

        try (MongoClient client = new MongoClient(new ServerAddress("localhost", serverAddress.getPort()), clientOptions)) {
            pingServer(client);
        }
    }

    @Test
    public void testEnableSslAfterAlreadyStarted() throws Exception {
        server.bind();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> server.enableSsl(getPrivateKey(), null, getCertificate()))
            .withMessage("Server already started");
    }

    @Test
    public void testEnableSslWithEmptyKeyCertChain() throws Exception {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> server.enableSsl(null, null))
            .withMessage("keyCertChain must be non-empty");
    }

    @Test
    public void testEnableSslWithMissingPrivateKey() throws Exception {
        X509Certificate certificate = getCertificate();

        assertThatExceptionOfType(NullPointerException.class)
            .isThrownBy(() -> server.enableSsl(null, null, certificate))
            .withMessage("key required for servers");
    }

    private PrivateKey getPrivateKey() throws Exception {
        return (PrivateKey) loadTestKeyStore().getKey("localhost", new char[0]);
    }

    private X509Certificate getCertificate() throws Exception {
        return (X509Certificate) loadTestKeyStore().getCertificate("localhost");
    }

    private KeyStore loadTestKeyStore() throws Exception {
        try (InputStream keyStoreStream = getClass().getResourceAsStream("/test-keystore.jks")) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(keyStoreStream, new char[0]);
            return keyStore;
        }
    }

    private SSLContext createSslContext(KeyStore keyStore) throws Exception {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private void pingServer(MongoClient client) {
        client.getDatabase("admin").runCommand(new Document("ping", 1));
    }

}
