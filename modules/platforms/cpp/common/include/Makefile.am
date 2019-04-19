#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
# 
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ACLOCAL_AMFLAGS =-I m4

nobase_include_HEADERS = \
	ignite/common/big_integer.h \
	ignite/common/bits.h \
	ignite/common/cancelable.h \
	ignite/common/concurrent.h \
	ignite/common/decimal.h \
	ignite/common/default_allocator.h \
	ignite/common/expected.h \
	ignite/common/reference_impl.h \
	ignite/common/dynamic_size_array.h \
	ignite/common/fixed_size_array.h \
	ignite/common/lazy.h \
	ignite/common/utils.h \
	ignite/common/platform_utils.h \
	ignite/common/shared_state.h \
	ignite/common/promise.h \
	ignite/date.h \
	ignite/guid.h \
	ignite/ignite_error.h \
	ignite/timestamp.h \
	ignite/time.h \
	ignite/reference.h \
	ignite/future.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
