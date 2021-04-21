/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IGNITE_ODBC_TEST_TEST_SERVER
#define _IGNITE_ODBC_TEST_TEST_SERVER

#include <stdint.h>

#include <vector>

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601
#endif // _WIN32_WINNT

#include <boost/asio.hpp>
#include <boost/thread.hpp>

namespace ignite
{

/**
 * Test Server Session.
 */
class TestServerSession
{
public:
    /**
     * Construct new instance of class.
     * @param service Asio service.
     * @param responses Responses to provide to requests.
     */
    TestServerSession(boost::asio::io_service& service, const std::vector< std::vector<int8_t> >& responses);

    /**
     * Get socket.
     */
    boost::asio::ip::tcp::socket& GetSocket()
    {
        return socket;
    }

    /**
     * Start session.
     */
    void Start();

    /**
     * Get response at index.
     * @param idx Index.
     * @return Response.
     */
    const std::vector<int8_t>& GetResponse(size_t idx) const
    {
        return responses.at(idx);
    }

private:
    /**
     * Receive next request.
     */
    void ReadNextRequest();

    /**
     * Handle received request size.
     * @param error Error.
     * @param bytesTransferred Bytes transferred.
     */
    void HandleRequestSizeReceived(const boost::system::error_code& error, size_t bytesTransferred);

    /**
     * Handle received request.
     * @param error Error.
     * @param bytesTransferred Bytes transferred.
     */
    void HandleRequestReceived(const boost::system::error_code& error, size_t bytesTransferred);

    /**
     * Handle received request.
     * @param error Error.
     * @param bytesTransferred Bytes transferred.
     */
    void HandleResponseSent(const boost::system::error_code& error, size_t bytesTransferred);

    // The socket used to communicate with the client.
    boost::asio::ip::tcp::socket socket;

    // Received requests.
    std::vector< std::vector<int8_t> > requests;

    // Responses to provide.
    const std::vector< std::vector<int8_t> > responses;

    // Number of requests answered.
    size_t requestsResponded;
};

/**
 * Test Server.
 */
class TestServer
{
public:
    /**
     * Constructor.
     * @param port TCP port to listen.
     */
    TestServer(uint16_t port = 11110);

    /**
     * Destructor.
     */
    ~TestServer();

    /**
     * Push new handshake response to send.
     * @param accept Accept or reject response.
     */
    void PushHandshakeResponse(bool accept)
    {
        std::vector<int8_t> rsp(4 + 1);
        rsp[0] = 1;
        rsp[4] = accept ? 1 : 0;

        PushResponse(rsp);
    }

    /**
     * Push new response to send.
     * @param resp Response to push.
     */
    void PushResponse(const std::vector<int8_t>& resp)
    {
        responses.push_back(resp);
    }

    /**
     * Get specified session.
     * @param idx Index.
     * @return Specified session.
     */
    TestServerSession& GetSession(size_t idx = 0)
    {
        return *sessions.at(idx);
    }

    /**
     * Start server.
     */
    void Start();

    /**
     * Stop server.
     */
    void Stop();

private:
    /**
     * Start accepting connections.
     */
    void StartAccept();

    /**
     * Handle accepted connection.
     * @param session Accepted session.
     * @param error Error.
     */
    void HandleAccept(boost::shared_ptr<TestServerSession> session, const boost::system::error_code& error);

    // Service.
    boost::asio::io_service service;

    // Acceptor.
    boost::asio::ip::tcp::acceptor acceptor;

    // Reponses.
    std::vector< std::vector<int8_t> > responses;

    // Sessions.
    std::vector< boost::shared_ptr<TestServerSession> > sessions;

    // Server Thread.
    boost::shared_ptr<boost::thread> serverThread;
};

} // namespace ignite

#endif //_IGNITE_ODBC_TEST_TEST_SERVER