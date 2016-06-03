'use strict';
var webpack = require('webpack');
var HtmlWebpackPlugin = require("html-webpack-plugin");
var path = require('path');
var glob = require('glob');
var jade = require('jade');

module.exports = function (_path) {
    var plugins = [
        new webpack.HotModuleReplacementPlugin(),
        new HtmlWebpackPlugin({
            filename: 'index.html',
            templateContent: function () {
                return jade.renderFile(path.join(_path, 'views', 'index.jade'));
            },
            title: 'DEBUG:Ignite Web Console'
        })
    ];

    glob.sync(path.join(_path, 'views') + '/**/*.jade').map(function (file) {
        plugins.push(
            new HtmlWebpackPlugin({
                filename: 'templates/'+path.basename(file).replace('.jade', '.html'),
                templateContent: function () {
                    return jade.renderFile(file);
                }
            }))
    });

    return {
        context: _path,
        debug: true,
        devtool: 'cheap-source-map',
        devServer: {
            contentBase: './dist',
            info: true,
            hot: true,
            inline: false,
            proxy: {
                '/api/*': {
                    target: 'http://localhost:3000'
                }
            },
            stats: {colors: true},
            port: 9000
        },

        plugins: plugins
    };
};
