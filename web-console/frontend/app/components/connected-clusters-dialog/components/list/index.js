

import './style.scss';
import templateUrl from './template.tpl.pug';
import controller from './controller';

export default {
    controller,
    templateUrl,
    bindings: {
        gridApi: '=?',
        data: '<?options'
    }
};
