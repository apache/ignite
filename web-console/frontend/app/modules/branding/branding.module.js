

import angular from 'angular';

import IgniteBranding from './branding.service';

import igniteTerms from './terms.directive';
import igniteFeatures from './features.directive';

angular
.module('ignite-console.branding', [
    'tf.metatags'
])
.service('IgniteBranding', IgniteBranding)
.config(['tfMetaTagsProvider', (tfMetaTagsProvider) => {
    tfMetaTagsProvider.setDefaults({
        title: 'Apache Ignite - Management Tool and Configuration Wizard',
        properties: {
            description: 'The Apache Ignite Web Console is an interactive management tool and configuration wizard which walks you through the creation of config files. Try it now.'
        }
    });

    tfMetaTagsProvider.setTitleSuffix(' – Apache Ignite Web Console');
}])
.directive('igniteTerms', igniteTerms)
.directive('igniteFeatures', igniteFeatures);
