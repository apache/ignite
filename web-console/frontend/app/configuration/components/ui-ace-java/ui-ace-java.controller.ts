

import IgniteUiAceGeneratorFactory from '../ui-ace.controller';

export default class IgniteUiAceJava extends IgniteUiAceGeneratorFactory {
    static $inject = ['$scope', '$attrs', 'IgniteVersion', 'JavaTransformer'];
}
