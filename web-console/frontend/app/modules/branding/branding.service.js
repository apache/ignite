

export default class {
    static $inject = ['IgniteVersion'];

    /**
     * @param {import('app/services/Version.service').default} Version
     */
    constructor(Version) {
        this.titleSuffix = ' - Apache Ignite Web Console';

        this.showIgniteLogo = false;

        this.footerHtml = [
            `<p></p>`,
            '<p></p>'            
        ].join('\n');

        this.termsState = null;

        this.featuresHtml = [
            '<p>Web Console is an interactive management tool which allows to:</p>',
            '<ul>',
            '   <li>Create and download cluster configurations</li>',
            '   <li>Automatically import domain model from any RDBMS</li>',
            '   <li>Connect to cluster and run SQL analytics on it</li>',
            '</ul>'
        ].join('\n');
    }
}
