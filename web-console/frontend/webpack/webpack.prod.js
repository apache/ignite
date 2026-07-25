

const merge = require('webpack-merge');

const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const UglifyJSPlugin = require('uglifyjs-webpack-plugin');

const commonCfg = require('./webpack.common');
const {devProdScss} = require('./styles');

module.exports = merge(commonCfg, {
    bail: true, // Cancel build on error.
    mode: 'production',
    module: {
        rules: [
            {
                test: /\.css$/,
                use: [MiniCssExtractPlugin.loader, 'css-loader']
            },
            ...devProdScss(false),
            {
                test: /\.html$/,
                use: 'file-loader'
            },
            {
                test: /\.(ts)$/,
                use: ['angular2-template-loader?keepUrl=true']
            }
        ]
    },
    plugins: [
        new MiniCssExtractPlugin({filename: 'assets/css/[name].[hash].css'})
    ],
    optimization: {
        minimizer: [
            new UglifyJSPlugin({
                uglifyOptions: {
                    keep_fnames: true,
                    keep_classnames: true
                }
            })
        ]
    }
});
