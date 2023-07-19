import angular from 'angular';
import {JavaTypesNonEnum} from './JavaTypesNonEnum.service';
import IgniteClusterDefaults from './generator/defaults/Cluster.service';
import IgniteCacheDefaults from './generator/defaults/Cache.service';
import IgniteEventGroups from './generator/defaults/Event-groups.service';
import IgniteConfigurationGenerator from './generator/ConfigurationGenerator';
import IgniteSpringTransformer from './generator/SpringTransformer.service';
import IgniteJavaTransformer from './generator/JavaTransformer.service';
import IgniteDockerGenerator from './generator/Docker.service';
import IgniteMavenGenerator from './generator/Maven.service';
import IgniteGeneratorProperties from './generator/Properties.service';
import IgniteReadmeGenerator from './generator/Readme.service';
import IgniteCustomGenerator from './generator/Custom.service';
import IgniteArtifactVersionUtils from './generator/ArtifactVersionChecker.service';


// Ignite events groups.
export default angular
    .module('ignite-console.configuration.generator', [])
    .service('JavaTypesNonEnum', JavaTypesNonEnum)
    .service('IgniteConfigurationGenerator', function() { return IgniteConfigurationGenerator;})
    .service('SpringTransformer', function() { return IgniteSpringTransformer;})
    .service('JavaTransformer', function() { return IgniteJavaTransformer;})    
    .service('IgniteEventGroups', IgniteEventGroups)
    .service('IgniteClusterDefaults', IgniteClusterDefaults)
    .service('IgniteCacheDefaults', IgniteCacheDefaults)
    .service('IgnitePropertiesGenerator', IgniteGeneratorProperties)
    .service('IgniteReadmeGenerator', IgniteReadmeGenerator)
    .service('IgniteDockerGenerator', IgniteDockerGenerator)
    .service('IgniteMavenGenerator', IgniteMavenGenerator)
    .service('IgniteCustomGenerator', IgniteCustomGenerator)
    .service('IgniteArtifactVersionUtils', IgniteArtifactVersionUtils);
