

import './style.scss';
import controller from './controller';

import templateUrl from './template.tpl.pug';

export default {
    templateUrl,
    controller,
    bindings: {
        gridApi: '<'
    }
};
