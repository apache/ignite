
import controller from './controller';
import templateUrl from './template.tpl.pug';
import './style.scss';

export default {
    controller,
    templateUrl,
    bindings: {
        service: '<',   
        onCall: '&'
    }
};