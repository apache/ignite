

import { Stacktrace } from 'app/types/index';

export default class PropertyItem {
    /** Stacktrace item to show. */
    cause: Stacktrace;

    /** Stacktrace expanded flag. */
    expanded: boolean;

    $onInit() {
        this.expanded = false;
    }

    toggleProperty() {
        this.expanded = !this.expanded;
    }
}
