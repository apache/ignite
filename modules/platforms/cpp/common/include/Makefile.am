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
	ignite/common/big_integer.h \
	ignite/common/bits.h \
	ignite/common/concurrent.h \
	ignite/common/decimal.h \
	ignite/common/default_allocator.h \
	ignite/common/reference_impl.h \
	ignite/common/dynamic_size_array.h \
	ignite/common/fixed_size_array.h \
	ignite/common/utils.h \
	ignite/common/platform_utils.h \
	ignite/date.h \
	ignite/guid.h \
	ignite/ignite_error.h \
	ignite/timestamp.h \
	ignite/reference.h

uninstall-hook:
	if [ -d ${includedir}/ignite ]; then find ${includedir}/ignite -type d -empty -delete; fi
