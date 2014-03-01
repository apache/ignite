/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Ssl {
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Net.Security;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using Dbg = System.Diagnostics.Debug;

    /**
     * <summary>
     * This class provides basic initialization of the SSL stream for
     * client-server communication.</summary>
     */
    public class GridClientSslContext : IGridClientSslContext {
        /** <summary>Allow all server certificates callback.</summary> */
        public static readonly RemoteCertificateValidationCallback AllowAllCerts = (i1, i2, i3, i4) => true;

        /** <summary>Deny all server certificates callback.</summary> */
        public static readonly RemoteCertificateValidationCallback DenyAllCerts = (i1, i2, i3, i4) => false;

        /** <summary>Validate certificates chain user-defined callback.</summary> */
        private RemoteCertificateValidationCallback callback;

        /** <summary>Constructs default context, which ignores any SSL errors.</summary> */
        public GridClientSslContext()
            : this(true) {
        }

        /**
         * <summary>
         * Constructs SSL context with permanent validation result.</summary>
         *
         * <param name="permanentResult">Permanent validation result: allow all certificates or deny all.</param>
         */
        public GridClientSslContext(bool permanentResult) {
            ValidateCallback = permanentResult ? AllowAllCerts : DenyAllCerts;

            ClientCertificates = new X509Certificate2Collection();
            EnabledSslProtocols = SslProtocols.Default;
            CheckCertificateRevocation = false;
        }

        /** <summary>Certificates collection to provide client SSL authentication. </summary> */
        public X509Certificate2Collection ClientCertificates {
            get;
            private set;
        }

        /** 
         * <summary>
         * The value that represents the protocol used for authentication 
         * (default <c>SslProtocols.Default</c>).</summary> 
         */
        public SslProtocols EnabledSslProtocols {
            get;
            set;
        }

        /** 
         * <summary>
         * The value that specifies whether the certificate revocation list is 
         * checked during authentication (default <c>false</c>).</summary> 
         */
        public bool CheckCertificateRevocation {
            get;
            set;
        }

        /** <summary>Validate certificates chain user-defined callback.</summary> */
        public RemoteCertificateValidationCallback ValidateCallback {
            get {
                return callback;
            }
            set {
                A.NotNull(value, "ValidateCallback");

                callback = value;
            }
        }

        /**
         * <summary>
         * Constructs SSL stream for the client.</summary>
         *
         * <param name="client">Tcp client for client-server communication.</param>
         * <returns>Configured SSL stream.</returns>
         */
        public SslStream CreateStream(TcpClient client) {
            var ep = client.Client.RemoteEndPoint as IPEndPoint;

            var stream = new SslStream(client.GetStream(), false, callback, null);

            if (ClientCertificates.Count == 0)
                stream.AuthenticateAsClient(ep.Address.ToString());
            else {
                stream.AuthenticateAsClient(ep.Address.ToString(), ClientCertificates, EnabledSslProtocols, CheckCertificateRevocation);
            }

            return stream;
        }
    }
}
