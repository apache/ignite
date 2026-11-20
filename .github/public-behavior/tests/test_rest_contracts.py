# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from contract_diff import compare_sources
from test_java_contracts import compare as compare_java


COMMANDS = "modules/core/src/main/java/org/apache/ignite/internal/processors/rest/GridRestCommand.java"
HANDLER = ("modules/rest-http/src/main/java/org/apache/ignite/internal/processors/rest/protocols/"
           "http/jetty/GridJettyRestHandler.java")
PROTOCOL = HANDLER.replace("GridJettyRestHandler", "GridJettyRestProtocol")


def compare(before, after):
    def snapshot(constants, cases, lookup=""):
        return {
            COMMANDS: ("package org.apache.ignite.internal.processors.rest; "
                       f"enum GridRestCommand {{ {constants}; GridRestCommand(String key) {{}} {lookup} }}"),
            HANDLER: ("package org.apache.ignite.internal.processors.rest.protocols.http.jetty; "
                      "class GridJettyRestHandler { Request createRequest(GridRestCommand cmd) { "
                      "Request request; switch (cmd) { " + cases + " default: throw new IllegalArgumentException(); } "
                      "return request; } }"),
        }

    old = snapshot(*before)
    new = snapshot(*after)
    pairs = [(path, path) for path in old if old[path] != new[path]]
    sources = {(side, path): source for side, files in (("before", old), ("after", new))
               for path, source in files.items()}
    return compare_sources(pairs, sources)


class RestContractsTest(unittest.TestCase):
    def test_endpoint_uses_the_context_and_the_registered_servlet_path(self):
        package = "package org.apache.ignite.internal.processors.rest.protocols.http.jetty; "
        handler = package + 'class GridJettyRestHandler { static final String IGNITE_CMD_PATH = "/ignite"; }'
        protocol = package + '''class GridJettyRestProtocol {
            GridJettyRestHandler jettyHnd;
            void loadJettyConfiguration() {
                ServletContextHandler mainCtx = new ServletContextHandler();
                mainCtx.setContextPath("/");
                mainCtx.addServlet(new ServletHolder(jettyHnd), IGNITE_CMD_PATH + "/*");
                hnds.addHandler(mainCtx);
            }
        }'''
        before = {HANDLER: handler, PROTOCOL: protocol}
        self.assertEqual(compare_java(before, {**before, HANDLER: handler.replace('"/ignite"', '"/rest"')}), [
            f"{HANDLER}\tChanged REST endpoint mapping from /ignite/* to /rest/*",
        ])
        self.assertEqual(compare_java(before, {**before, PROTOCOL: protocol.replace('("/")', '("/v2")')}), [
            f"{PROTOCOL}\tChanged REST endpoint mapping from /ignite/* to /v2/ignite/*",
        ])
        inline = protocol.replace('IGNITE_CMD_PATH + "/*"', '"/ignite/*"')
        self.assertEqual(compare_java(before, {HANDLER: handler.replace('"/ignite"', '"/unused"'), PROTOCOL: inline}), [])
        delegated = protocol.replace('hnds.addHandler(mainCtx);', 'registerOtherRoutes(mainCtx); hnds.addHandler(mainCtx);')
        self.assertEqual(compare_java(before, {HANDLER: handler.replace('"/ignite"', '"/v2"'), PROTOCOL: delegated}), [])

    def test_only_names_accepted_over_http_change_the_contract(self):
        before = ('CACHE_GET("get"), CACHE_PUT("put"), QUIT("quit"), @Deprecated OLD("old")',
                  'case CACHE_GET: case CACHE_PUT: case OLD: { request = build(); break; } case QUIT:')
        after = ('READ("get"), CACHE_PUT("write"), QUIT("exit"), NEW("new")',
                 'case READ: case CACHE_PUT: case NEW: { request = build(); break; } case QUIT:')
        self.assertEqual(compare(before, after), {COMMANDS: [
            "Removed REST command cmd=put", "Added REST command cmd=new", "Added REST command cmd=write",
        ]})

    def test_change_in_http_routing_alone_is_reported_against_the_handler(self):
        keys = 'CACHE_GET("get"), UNROUTED("other")'
        old = 'case CACHE_GET: request = build(); break;'
        new = 'case UNROUTED: request = build(); break;'
        self.assertEqual(compare((keys, old), (keys, new)), {HANDLER: [
            "Removed REST command cmd=get", "Added REST command cmd=other",
        ]})

    def test_leaves_computed_command_names_to_review(self):
        route = 'case CACHE_GET: return read();'
        self.assertEqual(compare(('CACHE_GET("get")', route), ('CACHE_GET(PREFIX + "et")', route)), {})

    def test_an_alias_can_preserve_a_command(self):
        route = 'case CACHE_GET: return read();'
        lookup = 'static GridRestCommand fromKey(String key) { return commands.get(key); }'
        alias = lookup.replace('return commands', 'if ("get".equals(key)) return CACHE_GET; return commands')
        self.assertEqual(compare(('CACHE_GET("get")', route, lookup), ('CACHE_GET("read")', route, alias)), {})

    def test_leaves_an_unrecognized_dispatch_to_review(self):
        self.assertEqual(compare(('CACHE_GET("get")', 'case CACHE_GET: return read();'),
                                 ('OTHER("fetch")', 'case OTHER: if (available()) return read();')), {})
