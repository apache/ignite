<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<!--
Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
and the EPL 1.0 (http://h2database.com/html/license.html).
Initial Developer: H2 Group
-->
<html><head>
    <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
    <title>${text.a.title}</title>
    <link rel="stylesheet" type="text/css" href="stylesheet.css" />
</head>
<body style="margin: 20px">
    <form name="adminLogin" method="post" action="admin.do?jsessionid=${sessionId}">
        <table class="login" cellspacing="0" cellpadding="0">
            <tr class="login">
                <th class="login">${text.adminLogin}</th>
                <th class="login"></th>
            </tr>
            <tr><td  class="login" colspan="2"></td></tr>
            <tr class="login">
                <td class="login">${text.a.password}:</td>
                <td class="login"><input type="password" name="password" value="" style="width:200px;" /></td>
            </tr>
            <tr class="login">
                <td class="login"></td>
                <td class="login">
                    <input type="submit" class="button" value="${text.adminLoginOk}" />
                    &nbsp;
                    <input type="button" class="button" value="${text.adminLoginCancel}" onclick="javascript:document.adminLogin.action='index.do?jsessionid=${sessionId}';submit()" />
                    <br />
                    <br />
                </td>
            </tr>
        </table>
        <br />
        <p class="error">${error}</p>
    </form>
    <script type="text/javascript">
        <!--
            document.adminLogin.password.focus();
        //-->
    </script>

</body></html>