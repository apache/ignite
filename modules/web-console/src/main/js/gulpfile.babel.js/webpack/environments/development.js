'use strict';

import webpack from 'webpack';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import {destDir, rootDir} from '../../paths';
import jade from 'jade';
import path from 'path';

module.exports = function () {


    let plugins = [
        new webpack.HotModuleReplacementPlugin(),
        new HtmlWebpackPlugin({
            filename: 'index.html',
            templateContent: function () {
                return jade.renderFile(path.join(rootDir, 'views', 'index.jade'));
            },
            title: 'DEBUG:Ignite Web Console'
        })
    ];


    return {
        context: rootDir,
        debug: true,
        devtool: 'eval',
        devServer: {
            historyApiFallback: true,
            publicPath: '/',
            contentBase: destDir,
            info: true,
            hot: true,
            inline: true,
            proxy: {
                '/socket.io': {
                    target: 'http://localhost:3000',
                    changeOrigin: true,
                    ws: true
                },
                '/api/v1/*': {
                    target: 'http://localhost:3000',
                    changeOrigin: true,
                    rewrite: function(req) {
                        req.url = req.url.replace(/^\/api\/v1/, '');
                        return req;
                    }
                }
            },
            stats: {colors: true},
            port: 9000
        },
        stats: {colors: true},
        plugins: plugins
    };
};
