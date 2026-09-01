

import {nonEmpty} from 'app/utils/lodashMixins';

export class ClusterSecrets {
    /** @type {String} */
    user;

    /** @type {String} */
    password;

    /** @type {String} */
    sessionToken;

    constructor() {
        this.user = 'ignite';
    }

    hasCredentials() {
        return nonEmpty(this.user) && nonEmpty(this.password);
    }

    resetCredentials() {
        this.resetSessionToken();

        this.password = null;
    }

    resetSessionToken() {
        this.sessionToken = null;
    }

    /**
     * @return {{sessionToken: String}|{'user': String, 'password': String}}
     */
    getCredentials() {
        const { sessionToken } = this;

        if (sessionToken)
            return { sessionToken };

        const { user, password } = this;

        return { user, password };
    }
}
