// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Ssl {
    using System.Net.Sockets;
    using System.Net.Security;

    /** <summary>This interface provides initialization of the SSL client-server communication.</summary> */
    public interface IGridClientSslContext {
        /**
         * <summary>
         * Constructs SSL stream for the client.</summary>
         *
         * <param name="client">Tcp client for client-server communication.</param>
         * <returns>Configured SSL stream.</returns>
         */
        SslStream CreateStream(TcpClient client);
    }
}
