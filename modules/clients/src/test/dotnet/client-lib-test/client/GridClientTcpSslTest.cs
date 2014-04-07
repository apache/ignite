// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Security.Cryptography.X509Certificates;
    using NUnit.Framework;

    using GridGain.Client.Ssl;

    [TestFixture]
    public class GridClientTcpSslTest : GridClientAbstractTest {
        override protected GridClientProtocol Protocol() {
            return GridClientProtocol.Tcp;
        }

        override protected IGridClientSslContext SslContext() {
            var sslCtx = new GridClientSslContext(true);

            sslCtx.ClientCertificates.Add(new X509Certificate2("cert\\client.pfx", "123456"));

            return sslCtx;
        }

        override protected String ServerAddress() {
            return Host + ":" + TcpSslPort;
        }
    }
}
