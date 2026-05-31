

/**
 * This is a workaround for the following issues:
 * https://github.com/TheLarkInn/angular2-template-loader/issues/86
 * https://github.com/webpack-contrib/raw-loader/issues/78
 */

const templateRegExp = /template:\s*require\(['"`].+['"`]\)/gm;

/**
 * @param {string} source
 * @param sourcemap
 */
module.exports = function(source, sourcemap) {
    this.cacheable && this.cacheable();

    const newSource = source.replace(templateRegExp, (match) => `${match}.default`);

    if (this.callback)
        this.callback(null, newSource, sourcemap);
    else
        return newSource;
};
