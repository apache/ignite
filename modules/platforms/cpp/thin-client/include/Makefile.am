##
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the "License"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##

ACLOCAL_AMFLAGS =-I m4

noinst_HEADERS = \
    ignite/thin/ssl_mode.h \
    ignite/thin/ignite_client.h \
    ignite/thin/ignite_client_configuration.h \
    ignite/impl/thin/parser.h \
    ignite/impl/thin/protocol_version.h \
    ignite/impl/thin/response_status.h \
    ignite/impl/thin/data_router.h \
    ignite/impl/thin/utility.h \
    ignite/impl/thin/net/end_point.h \
    ignite/impl/thin/net/tcp_socket_client.h \
    ignite/impl/thin/message.h \
    ignite/impl/thin/socket_client.h \
    ignite/impl/thin/ignite_client_impl.h \
    ignite/impl/thin/result_page.h \
    ignite/impl/thin/ssl/ssl_bindings.h \
    ignite/impl/thin/ssl/secure_socket_client.h \
    ignite/impl/thin/ssl/ssl_gateway.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
