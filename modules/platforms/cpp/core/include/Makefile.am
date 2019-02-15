##
##                   GridGain Community Edition Licensing
##                   Copyright 2019 GridGain Systems, Inc.
##
## Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
## Restriction;  you may not use this file except in compliance with the License. You may obtain a
## copy of the License at
##
## http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software distributed under the
## License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied. See the License for the specific language governing permissions
## and limitations under the License.
##
## Commons Clause Restriction
##
## The Software is provided to you by the Licensor under the License, as defined below, subject to
## the following condition.
##
## Without limiting other conditions in the License, the grant of rights under the License will not
## include, and the License does not grant to you, the right to Sell the Software.
## For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
## under the License to provide to third parties, for a fee or other consideration (including without
## limitation fees for hosting or consulting/ support services related to the Software), a product or
## service whose value derives, entirely or substantially, from the functionality of the Software.
## Any license notice or attribution required by the License must also include this Commons Clause
## License Condition notice.
##
## For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
## the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
## Edition software provided with this notice.
##

ACLOCAL_AMFLAGS =-I m4

nobase_include_HEADERS = \
	ignite/cache/cache.h \
	ignite/cache/cache_entry.h \
	ignite/cache/cache_entry_processor.h \
	ignite/cache/cache_peek_mode.h \
	ignite/cache/event/cache_entry_event.h \
	ignite/cache/event/cache_entry_event_filter.h \
	ignite/cache/event/cache_entry_event_listener.h \
	ignite/cache/mutable_cache_entry.h \
	ignite/cache/query/continuous/continuous_query.h \
	ignite/cache/query/continuous/continuous_query_handle.h \
	ignite/cache/query/query.h \
	ignite/cache/query/query_cursor.h \
	ignite/cache/query/query_fields_cursor.h \
	ignite/cache/query/query_fields_row.h \
	ignite/cache/query/query_scan.h \
	ignite/cache/query/query_sql.h \
	ignite/cache/query/query_sql_fields.h \
	ignite/cache/query/query_text.h \
	ignite/compute/compute.h \
	ignite/compute/compute_func.h \
	ignite/ignite.h \
	ignite/ignite_binding.h \
	ignite/ignite_binding_context.h \
	ignite/ignite_configuration.h \
	ignite/ignition.h \
	ignite/impl/binary/binary_type_updater_impl.h \
	ignite/impl/bindings.h \
	ignite/impl/cache/cache_entry_processor_holder.h \
	ignite/impl/cache/cache_impl.h \
	ignite/impl/cache/event/cache_entry_event_filter_base.h \
	ignite/impl/cache/event/cache_entry_event_filter_holder.h \
	ignite/impl/cache/query/continuous/continuous_query_handle_impl.h \
	ignite/impl/cache/query/continuous/continuous_query_impl.h \
	ignite/impl/cache/query/query_argument.h \
	ignite/impl/cache/query/query_batch.h \
	ignite/impl/cache/query/query_fields_row_impl.h \
	ignite/impl/cache/query/query_impl.h \
	ignite/impl/cluster/cluster_group_impl.h \
	ignite/impl/compute/cancelable_impl.h \
	ignite/impl/compute/compute_impl.h \
	ignite/impl/compute/compute_job_holder.h \
	ignite/impl/compute/compute_job_result.h \
	ignite/impl/compute/compute_task_holder.h \
	ignite/impl/compute/single_job_compute_task_holder.h \
	ignite/impl/compute/multiple_job_compute_task_holder.h \
	ignite/impl/handle_registry.h \
	ignite/impl/ignite_binding_impl.h \
	ignite/impl/ignite_environment.h \
	ignite/impl/ignite_impl.h \
	ignite/impl/interop/interop_external_memory.h \
	ignite/impl/interop/interop_target.h \
	ignite/impl/module_manager.h \
	ignite/impl/operations.h \
	ignite/impl/transactions/transactions_impl.h \
	ignite/impl/transactions/transaction_impl.h \
	ignite/transactions/transaction.h \
	ignite/transactions/transactions.h \
	ignite/transactions/transaction_consts.h \
	ignite/transactions/transaction_metrics.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
