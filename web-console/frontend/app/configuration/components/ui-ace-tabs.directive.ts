

import _ from 'lodash';

export default function() {
    return {
        scope: true,
        restrict: 'AE',
        controller: _.noop
    };
}
