<%--
  ~ Copyright 2019 GridGain Systems, Inc. and Contributors.
  ~ 
  ~ Licensed under the GridGain Community Edition License (the "License");
  ~ you may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~ 
  ~     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
  ~ 
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  --%>

<%@ page import="org.apache.ignite.Ignition" %>
<%@ page import="java.util.UUID" %>
<html>
<body>
<h2>Session ID</h2>
<%= request.getSession().getId() %>
<h2>Session content</h2>
<%= Ignition.ignite().cache("atomic").get(request.getSession().getId()) %>
<h2>Cache size</h2>
<%= Ignition.ignite().cache("atomic").size() %>

<%
    HttpSession ses = request.getSession();

    ses.setAttribute(UUID.randomUUID().toString(), "Value");
%>

</body>
</html>
