'use strict';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import webpack from 'webpack';
import path from 'path';
import jade from 'jade';

import {destDir, rootDir} from '../../paths';

module.exports = function (_path) {
    return {
        context: rootDir,
        debug: false,
        devtool: 'cheap-source-map',
        output: {
            publicPath: '/',
            filename: '[name].[chunkhash].js',
            contentBase: destDir
        },
        plugins: [
            new HtmlWebpackPlugin({
                filename: 'index.html',
                templateContent: () => {
                    return jade.renderFile(path.join(_path, 'views', 'index.jade'));
                },
                title: 'Ignite Web Console'
            }),
            new webpack.optimize.DedupePlugin()
        ]
    };
};