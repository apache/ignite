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

nobase_include_HEADERS = \
    ignite/ignite_configuration.h \
    ignite/ignite.h \
    ignite/impl/binary/binary_type_updater_impl.h \
    ignite/impl/operations.h \
    ignite/impl/ignite_environment.h \
    ignite/impl/ignite_impl.h \
    ignite/impl/cache/query/query_fields_row_impl.h \
    ignite/impl/cache/query/query_impl.h \
    ignite/impl/cache/cache_impl.h \
    ignite/impl/cache/query/query_batch.h \
    ignite/impl/interop/interop_target.h \
    ignite/impl/interop/interop_external_memory.h \
    ignite/impl/handle_registry.h \
    ignite/impl/transactions/transaction_impl.h \
    ignite/impl/transactions/transactions_impl.h \
    ignite/cache/query/query_fields_row.h \
    ignite/cache/query/query_fields_cursor.h \
    ignite/cache/query/query_scan.h \
    ignite/cache/query/query_cursor.h \
    ignite/cache/query/query_sql.h \
    ignite/cache/query/query.h \
    ignite/cache/query/query_argument.h \
    ignite/cache/query/query_sql_fields.h \
    ignite/cache/query/query_text.h \
    ignite/cache/cache.h \
    ignite/cache/cache_entry.h \
    ignite/cache/cache_peek_mode.h \
    ignite/ignition.h \
    ignite/transactions/transaction.h \
    ignite/transactions/transaction_consts.h \
    ignite/transactions/transactions.h \
    ignite/transactions/transaction_metrics.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
