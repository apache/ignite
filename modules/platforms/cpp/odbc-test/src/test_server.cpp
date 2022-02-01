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

#include <stdint.h>

#include <vector>

#ifdef _MSC_VER
#   pragma warning(push)
#   pragma warning(disable : 4355)
#endif //_MSC_VER

#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601
#endif // _WIN32_WINNT

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/make_shared.hpp>

#ifdef _MSC_VER
#   pragma warning(pop)
#endif //_MSC_VER

#include <ignite/binary/binary.h>
#include <ignite/impl/binary/binary_utils.h>

#include "test_server.h"

namespace ignite
{

TestServerSession::TestServerSession(boost::asio::io_service& service, const std::vector< std::vector<int8_t> >& responses) :
    socket(service),
    responses(responses),
    requestsResponded(0)
{
    // No-op.
}

void TestServerSession::Start()
{
    ReadNextRequest();
}

void TestServerSession::ReadNextRequest()
{
    requests.push_back(std::vector<int8_t>());

    std::vector<int8_t>& newRequest = requests.back();
    newRequest.resize(4);

    async_read(socket, boost::asio::buffer(newRequest.data(), newRequest.size()),
        boost::bind(&TestServerSession::HandleRequestSizeReceived, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
}

void TestServerSession::HandleRequestSizeReceived(const boost::system::error_code& error, size_t bytesTransferred)
{
    if (error || bytesTransferred != 4)
    {
        socket.close();

        return;
    }

    std::vector<int8_t>& newRequest = requests.back();
    impl::interop::InteropUnpooledMemory mem(4);
    mem.Length(4);

    memcpy(mem.Data(), newRequest.data(), newRequest.size());
    int32_t size = impl::binary::BinaryUtils::ReadInt32(mem, 0);

    newRequest.resize(4 + size);

    async_read(socket, boost::asio::buffer(newRequest.data() + 4, size),
        boost::bind(&TestServerSession::HandleRequestReceived, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
}

void TestServerSession::HandleRequestReceived(const boost::system::error_code& error, size_t bytesTransferred)
{
    if (error || !bytesTransferred || requestsResponded == responses.size())
    {
        socket.close();

        return;
    }

    const std::vector<int8_t>& response = responses.at(requestsResponded);

    async_write(socket, boost::asio::buffer(response.data(), response.size()),
        boost::bind(&TestServerSession::HandleResponseSent, this,
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));

    ++requestsResponded;
}

void TestServerSession::HandleResponseSent(const boost::system::error_code& error, size_t bytesTransferred)
{
    if (error || !bytesTransferred)
    {
        socket.close();

        return;
    }

    ReadNextRequest();
}


TestServer::TestServer(uint16_t port) :
    acceptor(service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
{
    // No-op.
}

TestServer::~TestServer()
{
    Stop();
}

void TestServer::Start()
{
    if (!serverThread)
    {
        StartAccept();
        serverThread.reset(new boost::thread(boost::bind(&boost::asio::io_service::run, &service)));
    }
}

void TestServer::Stop()
{
    if (serverThread)
    {
        service.stop();
        serverThread->join();
        serverThread.reset();
    }
}

void TestServer::StartAccept()
{
    using namespace boost::asio;

    boost::shared_ptr<TestServerSession> newSession;
    newSession.reset(new TestServerSession(service, responses));

    acceptor.async_accept(newSession->GetSocket(),
        boost::bind(&TestServer::HandleAccept, this, newSession, placeholders::error));
}

void TestServer::HandleAccept(boost::shared_ptr<TestServerSession> session, const boost::system::error_code& error)
{
    if (!error)
    {
        session->Start();

        sessions.push_back(session);
    }

    StartAccept();
}

} // namespace ignite
