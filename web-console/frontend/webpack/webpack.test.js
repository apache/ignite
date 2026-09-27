

const merge = require('webpack-merge');
const commonCfg = require('./webpack.common');

module.exports = merge(commonCfg, {
    mode: 'development',
    cache: true,
    node: {
        fs: 'empty',
        child_process: 'empty'
    },

    // Entry points.
    entry: null,

    // Output system.
    output: null,
    optimization: {
        splitChunks: {
            chunks: 'async'
        }
    },
    module: {
        exprContextCritical: false,
        rules: [
            {
                test: /\.(ts)$/,
                exclude: /\.spec\.ts$/,
                use: [
                    require.resolve('./fix-component-template-import-loader.js'),
                    'angular2-template-loader'
                ]
            },
            {
                test: /\.s?css$/,
                use: ['blank-loader']
            },
            {
                test: /\.(html)$/,
                use: 'raw-loader'
            }
        ]
    }
});
