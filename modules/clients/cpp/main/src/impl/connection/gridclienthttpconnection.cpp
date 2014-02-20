// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/spirit/include/classic.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "gridgain/impl/connection/gridclienthttpconnection.hpp"
#include "gridgain/impl/connection/gridclientconnectionpool.hpp"
#include "gridgain/impl/cmd/gridclientmessageauthrequestcommand.hpp"
#include "gridgain/impl/cmd/gridclientmessageauthresult.hpp"
#include "gridgain/impl/marshaller/json/gridclientjsonmarshaller.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"

using namespace std;

/**
 * Helper function for urlencode function.
 *
 * @param dec Character to encode.
 * @return URL encoded character.
 */
string char2hex(char dec) {
    char dig1 = (dec & 0xF0) >> 4;
    char dig2 = (dec & 0x0F);

    if (0 <= dig1 && dig1 <= 9)
        dig1 += 48;             //0,48 in ascii
    if (10 <= dig1 && dig1 <= 15)
        dig1 += 97 - 10;        //a,97 in ascii
    if (0 <= dig2 && dig2 <= 9)
        dig2 += 48;
    if (10 <= dig2 && dig2 <= 15)
        dig2 += 97 - 10;

    string r;

    r += dig1;
    r += dig2;

    return r;
}

/**
 * URL-encodes the string of characters.
 *
 * @param c String to encode.
 * @return URL-encoded string.
 */
string urlencode(const string& c) {
    string encoded = "";

    int max = c.length();

    for (int i = 0; i < max; ++i) {
        if ((48 <= c[i] && c[i] <= 57) || //0-9
                (65 <= c[i] && c[i] <= 90) || //abc...xyz
                (97 <= c[i] && c[i] <= 122) || //ABC...XYZ
                (c[i] == '~' || c[i] == '!' || c[i] == '*' || c[i] == '(' || c[i] == ')' || c[i] == '\'')) {
            encoded += c[i];
        } else {
            encoded += '%';
            encoded += char2hex(c[i]); //converts char 255 to string "ff"
        }
    }

    return encoded;
}

/**
 * Creates a string for HTTP request made of param pairs.
 *
 * @param params Key-value pairs.
 * @return HTTP GET request string.
 */
static string createHttpRequest(const TRequestParams& params) {
    string request("/gridgain?");

    for (auto it = params.begin(); it != params.end(); ++it) {
        if (it != params.begin())
            request += "&";

        request += urlencode(it->first) + "=" + urlencode(it->second);
    }

    return request;
}

/**
 * Reads HTTP response and returns it as string.
 *
 * @param socket Socket to read data from.
 * @return Response body.
 */
template<class T>
static string parseHttpResponse(T& socket) {
    static const unsigned int HTTP_STATUS_OK = 200;
    string httpVer;
    unsigned int statusCode;
    string statusMessage;
    stringstream responseBody;

    boost::asio::streambuf response;
    istream response_stream(&response);

    boost::asio::read_until(socket, response, "\r\n");

    response_stream >> httpVer;

    response_stream >> statusCode;

    getline(response_stream, statusMessage);

    if (!response_stream || httpVer.size() < 6 || httpVer.substr(0, 5) != "HTTP/") {
        GG_LOG_ERROR("Invalid http specification: %s", httpVer.c_str());

        throw runtime_error("Invalid specification");
    }

    if (statusCode != HTTP_STATUS_OK) {
        GG_LOG_ERROR("Invalid status code: %d", statusCode);

        throw runtime_error("Invalid status code");
    }

    // Read the response headers, which are terminated by a blank line.
    boost::asio::read_until(socket, response, "\r\n\r\n");

    // Skip all the headers information.
    string header;

    while (getline(response_stream, header) && header != "\r")
        ;

    // Write whatever content we already have to output.
    if (response.size() > 0)
        responseBody << &response;

    // Read until EOF, writing data to output as we go.
    boost::system::error_code error;

    while (boost::asio::read(socket, response, boost::asio::transfer_at_least(1), error))
        responseBody << &response;

    if (error != boost::asio::error::eof)
        throw boost::system::system_error(error);

    return responseBody.str();
}

/**
 * Sends a header of HTTP request to a stream.
 *
 * @param host Host name to use in the request header.
 * @param port Port to use in the request header.
 * @param pHttpRequest URL for HTTP GET.
 * @param request Stream to write request to.
 */
static void formHttpRequest(const char* host, int port, const string& pHttpRequest,
        boost::asio::streambuf& request) {
    ostream request_stream(&request);

    request_stream << "GET " << pHttpRequest << " HTTP/1.1\r\n";
    request_stream << "Host: " << host << ":" << port << "\r\n";
    request_stream << "Accept: */*\r\n";
    request_stream << "Connection: close\r\n\r\n";
}

/**
 * Connect to a host/port.
 *
 * @param pHost Host name.
 * @param pPort Port to connect to.
 */
void GridClientHttpConnection::connect(const string& pHost, int pPort) {
    try {
        boost::asio::io_service io_service;
        boost::asio::ip::tcp::resolver resolver(io_service);
        boost::asio::ip::tcp::resolver::query query(pHost, boost::lexical_cast<std::string>(pPort));

        // Resolve the host.
        boost::asio::ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

        // Connects to the required host.
        if (sslSock.get() == NULL)
            boost::asio::connect(getSocket(), endpoint_iterator);
        else {
            boost::asio::connect(sslSock.get()->lowest_layer(), endpoint_iterator);

            sslSock.get()->handshake(boost::asio::ssl::stream_base::client);
        }

        closed = false;

        assert(isOpen());

        host = pHost;
        port = pPort;

        GG_LOG_INFO("Connection established [host=%s, port=%d, protocol=HTTP]", host.c_str(), port);
    }
    catch (std::exception&) {
        if (closed)
            throw GridClientClosedException();

        throw;
    }
}

/**
 * Authenticate client and store session token for later use.
 *
 * @param clientId Client ID to authenticate.
 * @param creds Credentials to use.
 */
void GridClientHttpConnection::authenticate(string clientId, string creds) {
    GridAuthenticationRequestCommand authReq;
    GridClientMessageAuthenticationResult authResult;

    TRequestParams reqParams;

    boost::property_tree::ptree jsonObj;

    authReq.setClientId(clientId);
    authReq.credentials(creds);
    authReq.setRequestId(1);

    GridClientJsonMarshaller::wrap(authReq, reqParams);

    send(reqParams, jsonObj);

    GridClientJsonMarshaller::unwrap(jsonObj, authResult);

    sessToken = authResult.sessionToken();

    connect(host, port);
}

/**
 * Send a request to the host and receive reply.
 *
 * @param requestParams Key-value pairs.
 * @param jsonObj Boost property tree to be filled with response.
 */
void GridClientHttpConnection::send(TRequestParams requestParams, boost::property_tree::ptree& jsonObj) {
    try {
        // Forms http request.
        boost::asio::streambuf request;

        if (!sessToken.empty())
            requestParams["sessionToken"] = sessToken;

        string httpRequest(createHttpRequest(requestParams));

        formHttpRequest(host.c_str(), port, httpRequest, request);

        GG_LOG_DEBUG("Request sent [host=%s, port=%d, req=%s]", host.c_str(), port, httpRequest.c_str());

        string jsonObjAsString;

        // Sends the request.
        if (sslSock.get() == NULL) {
            boost::asio::write(getSocket(), request);

            // Gets the response from HTTP server.
            jsonObjAsString = parseHttpResponse(getSocket());

            close();
        } else {
            boost::asio::write(*sslSock.get(), request);

            jsonObjAsString = parseHttpResponse(*sslSock.get());
        }

        GG_LOG_DEBUG("Response received: %s", jsonObjAsString.c_str());

        // Prepare the stream for parsing of JSon parser.
        stringstream is;

        is << jsonObjAsString;

        // BOOST_SPIRIT_THREADSAFE is needed here, because this call is concurrent.
        boost::property_tree::json_parser::read_json(is, jsonObj);
    }
    catch(std::exception&) {
        if (closed)
            throw GridClientClosedException();

        throw;
    }
}
