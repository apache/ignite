

import { Stacktrace } from 'app/types/index';

export default class {
    static $inject = ['title', 'stacktrace'];

    /** Title of stacktrace view dialog. */
    title: string;

    /** Stacktrace elements to show. */
    stacktrace: Stacktrace;

    /**
     * @param title Title of stacktrace view dialog.
     * @param stacktrace Stacktrace elements to show.
     */
    constructor(title: string, stacktrace: Stacktrace) {
        this.title = title;
        this.stacktrace = stacktrace;
    }
}
