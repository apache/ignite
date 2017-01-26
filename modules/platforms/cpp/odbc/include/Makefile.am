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
    ignite/odbc.h \
    ignite/odbc/query/table_metadata_query.h \
    ignite/odbc/query/special_columns_query.h \
    ignite/odbc/query/type_info_query.h \
    ignite/odbc/query/data_query.h \
    ignite/odbc/query/foreign_keys_query.h \
    ignite/odbc/query/column_metadata_query.h \
    ignite/odbc/query/query.h \
    ignite/odbc/query/primary_keys_query.h \
    ignite/odbc/protocol_version.h \
    ignite/odbc/statement.h \
    ignite/odbc/config/configuration.h \
    ignite/odbc/config/connection_info.h \
    ignite/odbc/column.h \
    ignite/odbc/parser.h \
    ignite/odbc/app/application_data_buffer.h \
    ignite/odbc/app/parameter.h \
    ignite/odbc/row.h \
    ignite/odbc/utility.h \
    ignite/odbc/environment.h \
    ignite/odbc/dsn_config.h \
    ignite/odbc/system/odbc_constants.h \
    ignite/odbc/system/socket_client.h \
    ignite/odbc/meta/primary_key_meta.h \
    ignite/odbc/meta/column_meta.h \
    ignite/odbc/meta/table_meta.h \
    ignite/odbc/diagnostic/diagnostic_record.h \
    ignite/odbc/diagnostic/diagnostic_record_storage.h \
    ignite/odbc/diagnostic/diagnosable.h \
    ignite/odbc/diagnostic/diagnosable_adapter.h \
    ignite/odbc/connection.h \
    ignite/odbc/message.h \
    ignite/odbc/cursor.h \
    ignite/odbc/common_types.h \
    ignite/odbc/result_page.h \
    ignite/odbc/type_traits.h \
    ignite/odbc/log.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
