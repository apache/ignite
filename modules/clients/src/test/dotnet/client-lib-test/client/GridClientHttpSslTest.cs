// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.Generic;
    using System.Web.Script.Serialization;
    using NUnit.Framework;

    using GridGain.Client.Ssl;

    [TestFixture]
    public class GridClientHttpSslTest : GridClientAbstractTest {
        override protected GridClientProtocol Protocol() {
            return GridClientProtocol.Http;
        }

        override protected IGridClientSslContext SslContext() {
            return new GridClientSslContext(true);
        }

        override protected String ServerAddress() {
            return Host + ":" + HttpSslPort;
        }

        /** <inheritdoc /> */
        override protected String TaskName() {
            return "org.gridgain.client.GridClientHttpTask";
        }

        /** <inheritdoc /> */
        override protected Object TaskArgs(IList<String> list) {
            JavaScriptSerializer s = new JavaScriptSerializer();

            return s.Serialize(list);
        }
    }
}
