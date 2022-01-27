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

#include <memory>

#include <ignite/common/utils.h>

#include <ignite/network/network.h>
#include <ignite/network/ssl/secure_data_filter.h>

#include "network/ssl/ssl_gateway.h"
#include "network/ssl/secure_utils.h"

namespace ignite
{
    namespace network
    {
        namespace ssl
        {
            SecureDataFilter::SecureDataFilter(const SecureConfiguration &cfg) :
                sslContext(0),
                contexts(0),
                contextCs()
            {
                EnsureSslLoaded();

                sslContext = MakeContext(cfg);

                contexts = new ContextMap;
            }

            SecureDataFilter::~SecureDataFilter()
            {
                delete contexts;
                FreeContext(static_cast<SSL_CTX*>(sslContext));
            }

            bool SecureDataFilter::Send(uint64_t id, const DataBuffer &data)
            {
                SP_SecureConnectionContext context = FindContext(id);
                if (!context.IsValid())
                    return false;

                return context.Get()->Send(data);
            }

            void SecureDataFilter::OnConnectionSuccess(const EndPoint &addr, uint64_t id)
            {
                SP_SecureConnectionContext context(new SecureConnectionContext(id, addr, *this));

                {
                    common::concurrent::CsLockGuard lock(contextCs);

                    contexts->insert(std::make_pair(id, context));
                }

                context.Get()->DoConnect();
            }

            void SecureDataFilter::OnConnectionClosed(uint64_t id, const IgniteError *err)
            {
                SP_SecureConnectionContext context = FindContext(id);
                if (!context.IsValid())
                    return;

                if (context.Get()->IsConnected())
                    DataFilterAdapter::OnConnectionClosed(id, err);
                else
                {
                    std::stringstream sslErrMsg;
                    sslErrMsg << "Connection closed during SSL/TLS handshake";
                    if (err)
                        sslErrMsg << ". Details: " << err->GetText();

                    std::string sslErrMsgStr = sslErrMsg.str();

                    IgniteError err0(IgniteError::IGNITE_ERR_SECURE_CONNECTION_FAILURE, sslErrMsgStr.c_str());

                    DataFilterAdapter::OnConnectionError(context.Get()->GetAddress(), err0);
                }

                {
                    common::concurrent::CsLockGuard lock(contextCs);

                    contexts->erase(id);
                }
            }

            void SecureDataFilter::OnMessageReceived(uint64_t id, const DataBuffer &msg)
            {
                SP_SecureConnectionContext context = FindContext(id);
                if (!context.IsValid())
                    return;

                SecureConnectionContext& context0 = *context.Get();

                DataBuffer in(msg);

                while (!in.IsEmpty())
                {
                    bool connectionHappened = context0.ProcessData(in);

                    if (connectionHappened)
                        DataFilterAdapter::OnConnectionSuccess(context0.GetAddress(), id);

                    if (context0.IsConnected())
                    {
                        DataBuffer data = context0.GetPendingDecryptedData();
                        while (!data.IsEmpty())
                        {
                            DataFilterAdapter::OnMessageReceived(id, data);
                            data = context0.GetPendingDecryptedData();
                        }
                    }
                }
            }

            SecureDataFilter::SP_SecureConnectionContext SecureDataFilter::FindContext(uint64_t id)
            {
                common::concurrent::CsLockGuard lock(contextCs);

                std::map<uint64_t, SP_SecureConnectionContext>::iterator it = contexts->find(id);
                if (it == contexts->end())
                    return SP_SecureConnectionContext();

                return it->second;
            }

            bool SecureDataFilter::SendInternal(uint64_t id, const DataBuffer& data)
            {
                return DataFilterAdapter::Send(id, data);
            }

            SecureDataFilter::SecureConnectionContext::SecureConnectionContext(
                uint64_t id,
                const EndPoint &addr,
                SecureDataFilter& filter
            ) :
                connected(false),
                id(id),
                addr(addr),
                filter(filter),
                ssl(0),
                bioIn(0),
                bioOut(0)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                ssl = sslGateway.SSL_new_(static_cast<SSL_CTX*>(filter.sslContext));
                if (!ssl)
                    ThrowLastSecureError("Can not create secure connection");

                bioIn = sslGateway.BIO_new_(sslGateway.BIO_s_mem_());
                if (!bioIn)
                    ThrowLastSecureError("Can not create input BIO");

                bioOut = sslGateway.BIO_new_(sslGateway.BIO_s_mem_());
                if (!bioOut)
                    ThrowLastSecureError("Can not create output BIO");

                sslGateway.SSL_set_bio_(static_cast<SSL*>(ssl), static_cast<BIO*>(bioIn), static_cast<BIO*>(bioOut));
                sslGateway.SSL_set_connect_state_(static_cast<SSL*>(ssl));
            }

            SecureDataFilter::SecureConnectionContext::~SecureConnectionContext()
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                if (ssl)
                    sslGateway.SSL_free_(static_cast<SSL*>(ssl));
                else
                {
                    if (bioIn)
                        sslGateway.BIO_free_all_(static_cast<BIO*>(bioIn));

                    if (bioOut)
                        sslGateway.BIO_free_all_(static_cast<BIO*>(bioOut));
                }
            }

            bool SecureDataFilter::SecureConnectionContext::DoConnect()
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                SSL* ssl0 = static_cast<SSL*>(ssl);
                int res = sslGateway.SSL_connect_(ssl0);

                if (res != SSL_OPERATION_SUCCESS)
                {
                    int sslError = sslGateway.SSL_get_error_(ssl0, res);
                    if (IsActualError(sslError))
                        ThrowLastSecureError("Can not establish secure connection");
                }

                SendPendingData();

                return res == SSL_OPERATION_SUCCESS;
            }

            bool SecureDataFilter::SecureConnectionContext::SendPendingData()
            {
                DataBuffer data(GetPendingData(bioOut));

                if (data.IsEmpty())
                    return false;

                return filter.SendInternal(id, data);
            }

            bool SecureDataFilter::SecureConnectionContext::Send(const DataBuffer& data)
            {
                if (!connected)
                    return false;

                SslGateway &sslGateway = SslGateway::GetInstance();

                int res = sslGateway.SSL_write_(static_cast<SSL*>(ssl), data.GetData(), data.GetSize());
                if (res <= 0)
                    return false;

                return SendPendingData();
            }

            bool SecureDataFilter::SecureConnectionContext::ProcessData(DataBuffer& data)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();
                int res = sslGateway.BIO_write_(static_cast<BIO*>(bioIn), data.GetData(), data.GetSize());
                if (res <= 0)
                    ThrowLastSecureError("Failed to process SSL data");

                data.Skip(res);

                SendPendingData();

                if (connected)
                    return false;

                connected = DoConnect();

                SendPendingData();

                if (!connected)
                    return false;

                recvBuffer = impl::interop::SP_InteropMemory(
                    new impl::interop::InteropUnpooledMemory(RECEIVE_BUFFER_SIZE));

                return true;
            }

            DataBuffer SecureDataFilter::SecureConnectionContext::GetPendingData(void* bio)
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                BIO *bio0 = static_cast<BIO*>(bio);
                int available = sslGateway.BIO_pending_(bio0);

                impl::interop::SP_InteropMemory buf(new impl::interop::InteropUnpooledMemory(available));
                buf.Get()->Length(available);

                int res = sslGateway.BIO_read_(bio0, buf.Get()->Data(), buf.Get()->Length());
                if (res <= 0)
                    return DataBuffer();

                return DataBuffer(buf);
            }

            DataBuffer SecureDataFilter::SecureConnectionContext::GetPendingDecryptedData()
            {
                SslGateway &sslGateway = SslGateway::GetInstance();

                SSL *ssl0 = static_cast<SSL*>(ssl);
                int res = sslGateway.SSL_read_(ssl0, recvBuffer.Get()->Data(), recvBuffer.Get()->Capacity());
                if (res <= 0)
                    return DataBuffer();

                recvBuffer.Get()->Length(res);
                return DataBuffer(recvBuffer);
            }
        }
    }
}
