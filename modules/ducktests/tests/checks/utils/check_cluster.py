# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Checks custom cluster metadata decorator.
"""

from unittest.mock import Mock

import pytest
from ducktape.cluster.cluster_spec import ClusterSpec, LINUX
from ducktape.mark.mark_expander import MarkedFunctionExpander

from ignitetest.utils._mark import cluster, ParametrizableClusterMetadata, CLUSTER_SIZE_KEYWORD, CLUSTER_SPEC_KEYWORD


def expand_function(*, func, sess_ctx):
    """
    Inject parameters into function and generate context list.
    """
    assert hasattr(func, "marks")
    assert next(filter(lambda x: isinstance(x, ParametrizableClusterMetadata), func.marks), None)

    return MarkedFunctionExpander(session_context=sess_ctx, function=func).expand()


def mock_session_ctx(*, cluster_size=None):
    """
    Create mock of session context.
    """
    sess_ctx = Mock()
    sess_ctx.globals = {"cluster_size": cluster_size} if cluster_size is not None else {}

    return sess_ctx


class CheckClusterParametrization:
    """
    Checks custom @cluster parametrization.
    """
    def check_num_nodes(self):
        """"
        Check num_nodes.
        """
        @cluster(num_nodes=10)
        def function():
            return 0

        test_context_list = expand_function(func=function, sess_ctx=mock_session_ctx())
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_use_metadata[CLUSTER_SIZE_KEYWORD] == 10

        test_context_list = expand_function(func=function,
                                            sess_ctx=mock_session_ctx(cluster_size="100"))
        assert len(test_context_list) == 1
        assert test_context_list[0].cluster_use_metadata[CLUSTER_SIZE_KEYWORD] == 100

    def check_cluster_spec(self):
        """"
        Check cluster_spec.
        """
        @cluster(cluster_spec=ClusterSpec.simple_linux(10))
        def function():
            return 0

        test_context_list = expand_function(func=function, sess_ctx=mock_session_ctx())
        assert len(test_context_list) == 1
        inserted_spec = test_context_list[0].cluster_use_metadata[CLUSTER_SPEC_KEYWORD]

        assert inserted_spec.size() == 10
        for node in inserted_spec.nodes:
            assert node.operating_system == LINUX

        test_context_list = expand_function(func=function,
                                            sess_ctx=mock_session_ctx(cluster_size="100"))
        assert len(test_context_list) == 1
        inserted_spec = test_context_list[0].cluster_use_metadata[CLUSTER_SPEC_KEYWORD]

        assert inserted_spec.size() == 100
        for node in inserted_spec.nodes:
            assert node.operating_system == LINUX

    def check_invalid_global_param(self):
        """Check handle of invalid params."""
        @cluster(num_nodes=10)
        def function():
            return 0

        invalid_vals = ["abc", "-10", "1.5", "0", 1.6, -7, 0]

        for val in invalid_vals:
            with pytest.raises(Exception):
                expand_function(func=function, sess_ctx=mock_session_ctx(cluster_size=val))
