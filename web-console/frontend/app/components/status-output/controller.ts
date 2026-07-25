

import {StatusOptions, StatusOption} from './index';

interface Changes extends ng.IOnChangesObject {
    value: ng.IChangesObject<string>,
    options: ng.IChangesObject<StatusOptions>
}

const UNIVERSAL_CLASSNAME = 'status-output';

export class Status implements ng.IComponentController, ng.IOnChanges, ng.IPostLink, ng.IOnDestroy {
    static $inject = ['$element'];

    value: string;
    options: StatusOptions;
    status: StatusOption | undefined;
    statusClassName: string | undefined;

    constructor(private el: JQLite) {}

    $postLink() {
        this.el[0].classList.add(UNIVERSAL_CLASSNAME);
    }

    $onDestroy() {
        delete this.el;
    }

    $onChanges(changes: Changes) {
        if ('value' in changes) {
            this.status = this.options.find((option) => option.value === this.value);

            if (this.status)
                this.statusClassName = `${UNIVERSAL_CLASSNAME}__${this.status.level.toLowerCase()}`;
        }
    }
}
