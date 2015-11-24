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

ACLOCAL_AMFLAGS = "-Im4"

nobase_include_HEADERS = ignite/cache/cache.h \
                         ignite/cache/cache_entry.h \
                         ignite/cache/cache_peek_mode.h \
                         ignite/cache/query/query_argument.h \
                         ignite/cache/query/query_cursor.h \
                         ignite/cache/query/query_fields_cursor.h \
                         ignite/cache/query/query_fields_row.h \
                         ignite/cache/query/query_scan.h \
                         ignite/cache/query/query_sql.h \
                         ignite/cache/query/query_sql_fields.h \
                         ignite/cache/query/query_text.h \
                         ignite/cache/query/query.h \
                         ignite/impl/cache/cache_impl.h \
                         ignite/impl/cache/query/query_impl.h \
                         ignite/impl/cache/query/query_fields_row_impl.h \
                         ignite/impl/interop/interop.h \
                         ignite/impl/interop/interop_input_stream.h \
                         ignite/impl/interop/interop_memory.h \
                         ignite/impl/interop/interop_output_stream.h \
                         ignite/impl/binary/binary_type_handler.h \
                         ignite/impl/binary/binary_type_manager.h \
                         ignite/impl/binary/binary_type_snapshot.h \
                         ignite/impl/binary/binary_type_updater.h \
                         ignite/impl/binary/binary_type_updater_impl.h \
                         ignite/impl/binary/binary_common.h \
                         ignite/impl/binary/binary_id_resolver.h \
                         ignite/impl/binary/binary_reader_impl.h \
                         ignite/impl/binary/binary_utils.h \
                         ignite/impl/binary/binary_writer_impl.h \
                         ignite/impl/binary/binary_schema.h \
                         ignite/impl/ignite_environment.h \
                         ignite/impl/ignite_impl.h \
                         ignite/impl/handle_registry.h \
                         ignite/impl/operations.h \
                         ignite/binary/binary.h \
                         ignite/binary/binary_consts.h \
                         ignite/binary/binary_containers.h \
                         ignite/binary/binary_type.h \
                         ignite/binary/binary_raw_reader.h \
                         ignite/binary/binary_raw_writer.h \
                         ignite/binary/binary_reader.h \
                         ignite/binary/binary_writer.h \
                         ignite/ignite.h \
                         ignite/ignite_configuration.h \
                         ignite/ignite_error.h \
                         ignite/ignition.h \
                         ignite/guid.h

uninstall-hook:
	find ${includedir}/ignite -type d -empty -delete
