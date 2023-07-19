

import JSZip from 'jszip';

import PageConfigure from '../../services/PageConfigure';
import ConfigurationResourceFactory from '../../services/ConfigurationResource';
import SummaryZipperFactory from '../../services/SummaryZipper';
import IgniteVersion from 'app/services/Version.service';
import ConfigurationDownload from '../../services/ConfigurationDownload';
import IgniteLoadingFactory from 'app/modules/loading/loading.service';
import MessagesFactory from 'app/services/Messages.service';
import {Cluster, ShortCluster} from '../../types';

type CluserLike = Cluster | ShortCluster;

export default class ModalPreviewProjectController {
    static $inject = [
        'PageConfigure',
        'IgniteConfigurationResource',
        'IgniteSummaryZipper',
        'IgniteVersion',
        '$scope',
        'ConfigurationDownload',
        'IgniteLoading',
        'IgniteMessages'
    ];

    constructor(
        private PageConfigure: PageConfigure,
        private IgniteConfigurationResource: ReturnType<typeof ConfigurationResourceFactory>,
        private summaryZipper: ReturnType<typeof SummaryZipperFactory>,
        private IgniteVersion: IgniteVersion,
        private $scope: ng.IScope,
        private ConfigurationDownload: ConfigurationDownload,
        private IgniteLoading: ReturnType<typeof IgniteLoadingFactory>,
        private IgniteMessages: ReturnType<typeof MessagesFactory>
    ) {}

    onHide: ng.ICompiledExpression;
    cluster: CluserLike;
    isDemo: boolean;
    fileText: string;

    $onInit() {
        this.treeOptions = {
            nodeChildren: 'children',
            dirSelectable: false,
            injectClasses: {
                iExpanded: 'fa fa-folder-open-o',
                iCollapsed: 'fa fa-folder-o'
            }
        };
        this.doStuff(this.cluster, this.isDemo);
    }

    showPreview(node) {
        this.fileText = '';

        if (!node)
            return;

        this.fileExt = node.file.name.split('.').reverse()[0].toLowerCase();

        if (node.file.dir)
            return;

        node.file.async('string').then((text) => {
            this.fileText = text;
            this.$scope.$applyAsync();
        });
    }

    doStuff(cluster: CluserLike, isDemo: boolean) {
        this.IgniteLoading.start('projectStructurePreview');

        return this.PageConfigure.getClusterConfiguration({clusterID: cluster.id, isDemo})
        .then((data) => {
            return this.IgniteConfigurationResource.populate(data);
        })
        .then(({clusters}) => {
            return clusters.find(({id}) => id === cluster.id);
        })
        .then((cluster) => {
            return this.summaryZipper({
                cluster,
                data: {},
                demoMode: isDemo,
                targetVer: this.IgniteVersion.currentSbj.getValue()
            });
        })
        .then(JSZip.loadAsync)
        .then((val) => {
            const convert = (files) => {
                return Object.keys(files)
                .map((path, i, paths) => ({
                    fullPath: path,
                    path: path.replace(/\/$/, ''),
                    file: files[path],
                    parent: files[paths.filter((p) => path.startsWith(p) && p !== path).sort((a, b) => b.length - a.length)[0]]
                }))
                .map((node, i, nodes) => Object.assign(node, {
                    path: node.parent ? node.path.replace(node.parent.name, '') : node.path,
                    children: nodes.filter((n) => n.parent && n.parent.name === node.file.name)
                }));
            };

            const nodes = convert(val.files);

            this.data = [{
                path: this.ConfigurationDownload.nameFile(cluster),
                file: {dir: true},
                children: nodes.filter((n) => !n.parent)
            }];

            this.selectedNode = nodes.find((n) => n.path.includes('server.xml'));
            this.expandedNodes = [
                ...this.data,
                ...nodes.filter((n) => {
                    return !n.fullPath.startsWith('src/main/java/')
                        || /src\/main\/java(\/(config|load|startup))?\/$/.test(n.fullPath);
                })
            ];
            this.showPreview(this.selectedNode);
            this.IgniteLoading.finish('projectStructurePreview');
        })
        .catch((e) => {
            this.IgniteMessages.showError('Failed to generate project preview: ', e);
            this.onHide({});
        });
    }

    orderBy() {
        return;
    }
}
