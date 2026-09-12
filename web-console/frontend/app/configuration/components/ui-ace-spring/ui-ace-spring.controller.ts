

import IgniteUiAceGeneratorFactory from '../ui-ace.controller';

export default class IgniteUiAceSpring extends IgniteUiAceGeneratorFactory {
    static $inject = ['$scope', '$attrs', 'IgniteVersion', 'SpringTransformer'];
}
