/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/gridclientprotocolconfiguration.hpp"

using namespace std;

/**
 * Implementation class (pimpl).
 */
class GridClientProtocolConfiguration::Impl {
public:
    Impl() : maxConnIdleTime_(DFLT_MAX_CONN_IDLE_TIME), 
        connTimeout_(DFLT_MAX_CONN_TIMEOUT),
        protType_(DFLT_CLIENT_PROTOCOL), sslEnabled_(false), cred_(""), uuid_(GridUuid::randomUuid()) {
    }

    Impl(const Impl& other) : maxConnIdleTime_(other.maxConnIdleTime_), connTimeout_(other.connTimeout_), 
        protType_(other.protType_), sslEnabled_(other.sslEnabled_), cred_(other.cred_), cert_(other.cert_), 
        pass_(other.pass_), uuid_(other.uuid_) {
    }

    /** Max connection idle time. */
    long maxConnIdleTime_;

    /** Connection timeout. */
    int connTimeout_;

    /** Protocol. */
    GridClientProtocol protType_;

    /** Ssl enabled flag. */
    bool sslEnabled_;

    /** Credentials. */
    string cred_;

    /** Path to certificate file. */
    string cert_;

    /** Certificate file password. */
    string pass_;

    /** Client Uuid. */
    GridUuid uuid_;
};


/** Default public constructor. */
GridClientProtocolConfiguration::GridClientProtocolConfiguration() : pimpl(new Impl) {}

/**
* Copy constructor.
*
* @param other Another instance of configuration.
*/
GridClientProtocolConfiguration::GridClientProtocolConfiguration(const GridClientProtocolConfiguration& other) 
    : pimpl(new Impl(*other.pimpl)) {}

GridClientProtocolConfiguration& GridClientProtocolConfiguration::operator=(const GridClientProtocolConfiguration& rhs){
    if (this != &rhs) {
        delete pimpl;

        pimpl=new Impl(*rhs.pimpl);
    }

    return *this;
}

/** Destructor. */
GridClientProtocolConfiguration::~GridClientProtocolConfiguration() {
    delete pimpl;
}

/**
* Gets protocol for communication between client and remote grid.
*
* @return Protocol for communication between client and remote grid.
*/
GridClientProtocol GridClientProtocolConfiguration::protocol() const {
    return pimpl->protType_;
};

/**
* Sets protocol for communication between client and remote grid.
*
* @param p Protocol for communication between client and remote grid.
*/
void GridClientProtocolConfiguration::protocol(GridClientProtocol p) {
    pimpl->protType_ = p;
};

/**
* Gets timeout for socket connect operation.
*
* @return Socket connect timeout in milliseconds.
*/
int GridClientProtocolConfiguration::connectTimeout() const {
    return pimpl->connTimeout_;
}

/**
* Sets timeout for socket connect operation.
*
* @param t Socket connect timeout in milliseconds.
*/
void GridClientProtocolConfiguration::connectTimeout(int t) {
    pimpl->connTimeout_ = t;
}

/**
* Flag indicating whether client should try to connect server with secure
* socket layer enabled (regardless of protocol used).
*
* @return {@code True} if SSL should be enabled.
*/
bool GridClientProtocolConfiguration::sslEnabled() const {
    return pimpl->sslEnabled_;
}

/**
* Flag indicating whether client should try to connect server with secure
* socket layer enabled (regardless of protocol used).
*
* @param ssl New value for the flag.
*/
void GridClientProtocolConfiguration::sslEnabled(bool ssl) {
    pimpl->sslEnabled_ = ssl;
}

/**
* Path to file where SSL certificate is stored. If path is not absolute, it's considered
* relative to {@code GRIDGAIN_HOME}.
*
* @return path to file.
*/
string GridClientProtocolConfiguration::certificateFilePath() const {
    return pimpl->cert_;
}

/**
* Path to file where SSL certificate is stored. If path is not absolute, it's considered
* relative to {@code GRIDGAIN_HOME}.
*
* @param cert New value.
*/
void GridClientProtocolConfiguration::certificateFilePath(const string& cert) {
    pimpl->cert_ = cert;
}

/**
* Password for the certificate.
*
* @return Password.
*/
string GridClientProtocolConfiguration::certificateFilePassword() const {
    return pimpl->pass_;
}

/**
* Password for the certificate.
*
* @param pass New password.
*/
void GridClientProtocolConfiguration::certificateFilePassword(const string& pass) {
    pimpl->pass_ = pass;
}

/**
* Returns UUID used to authenticate
*/
GridUuid GridClientProtocolConfiguration::uuid() const {
    return pimpl->uuid_;
}

/**
* Gets client credentials to authenticate with.
*
* @return Credentials.
*/
string GridClientProtocolConfiguration::credentials() const {
    return pimpl->cred_;
}

/**
* Sets client credentials to authenticate with.
*
* @param cred New value for the credentials.
*/
void GridClientProtocolConfiguration::credentials(const string& cred) {
    pimpl->cred_ = cred;
}

/**
* Gets maximum amount of time that client connection can be idle before it is closed.
*
* @return Maximum idle time in milliseconds.
*/
long GridClientProtocolConfiguration::maxConnectionIdleTime() const {
    return pimpl->maxConnIdleTime_;
}

/**
* Sets maximum amount of time that client connection can be idle before it is closed.
*
* @param maxTime Maximum idle time in milliseconds.
*/
void GridClientProtocolConfiguration::maxConnectionIdleTime(long maxTime) {
    pimpl->maxConnIdleTime_ = maxTime;
}
