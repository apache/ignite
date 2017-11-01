package org.apache.ignite.console.agent.rest;

import java.io.IOException;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

public class RestExecutorSecurity extends RestExecutor {
    /** */
    private String sesTok;

    /** */
    private String login;

    /** */
    private String pwd;

    /**
     * @param login Node login.
     * @param pwd Node password.
     */
    public RestExecutorSecurity(String login, String pwd) {
        this.login = login;
        this.pwd = pwd;
    }

    /** */
    private boolean hasCredentials() {
        return !F.isEmpty(login) && !F.isEmpty(pwd);
    }

    /** {@inheritDoc} */
    @Override public RestResult sendRequest(String url, Map<String, Object> params,
        Map<String, Object> headers) throws IOException {

        if (!F.isEmpty(sesTok))
            params.put("sessionToken", sesTok);

        RestResult res = super.sendRequest(url, params, headers);

        if (res.getStatus() == STATUS_AUTH_FAILED && hasCredentials()) {
            params.put("ignite.login", login);
            params.put("ignite.password", pwd);

            res = super.sendRequest(url, params, null);

            if (res.getStatus() == STATUS_SUCCESS)
                sesTok = res.getSessionToken();
        }

        return res;
    }
}
