#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

void main()
{
    //tag::thin-client-ssl[]
    IgniteClientConfiguration cfg;

    // Sets SSL mode.
    cfg.SetSslMode(SslMode::Type::REQUIRE);

    // Sets file path to SSL certificate authority to authenticate server certificate during connection establishment.
    cfg.SetSslCaFile("path/to/SSL/certificate/authority");

    // Sets file path to SSL certificate to use during connection establishment.
    cfg.SetSslCertFile("path/to/SSL/certificate");

    // Sets file path to SSL private key to use during connection establishment.
    cfg.SetSslKeyFile("path/to/SSL/private/key");
    //end::thin-client-ssl[]
}
