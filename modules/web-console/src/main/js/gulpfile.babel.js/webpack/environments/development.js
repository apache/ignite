'use strict';

import webpack from 'webpack';
import ProgressPlugin from 'webpack/lib/ProgressPlugin';
import HtmlWebpackPlugin from 'html-webpack-plugin';
import {destDir, rootDir} from '../../paths';
import jade from 'jade';
import path from 'path';

module.exports = function () {
    let chars = 0,
        lastState, lastStateTime;

    const _goToLineStart = (nextMessage) => {
        var str = "";
        for (; chars > nextMessage.length; chars--) {
            str += "\b \b";
        }
        chars = nextMessage.length;
        for (var i = 0; i < chars; i++) {
            str += "\b";
        }
        if (str) process.stderr.write(str);
    };

    let plugins = [
        new webpack.HotModuleReplacementPlugin(),
        new HtmlWebpackPlugin({
            filename: 'index.html',
            templateContent: function () {
                return jade.renderFile(path.join(rootDir, 'views', 'index.jade'));
            },
            title: 'DEBUG:Ignite Web Console'
        }),
        new ProgressPlugin((percentage, msg) => {
            var state = msg;
            if (percentage < 1) {
                percentage = Math.floor(percentage * 100);
                msg = percentage + "% " + msg;
                if (percentage < 100) {
                    msg = " " + msg;
                }
                if (percentage < 10) {
                    msg = " " + msg;
                }
            }
            state = state.replace(/^\d+\/\d+\s+/, "");
            if (percentage === 0) {
                lastState = null;
                lastStateTime = +new Date();
            } else if (state !== lastState || percentage === 1) {
                var now = +new Date();
                if (lastState) {
                    var stateMsg = (now - lastStateTime) + "ms " + lastState;
                    _goToLineStart(stateMsg);
                    process.stderr.write(stateMsg + "\n");
                    chars = 0;
                }
                lastState = state;
                lastStateTime = now;
            }
            _goToLineStart(msg);
            process.stderr.write(msg);
        })
    ];


    return {
        context: rootDir,
        debug: true,
        devtool: 'eval',
        devServer: {
            historyApiFallback: true,
            // publicPath: '/',
            contentBase: destDir,
            // info: true,
            // hot: true,
            // inline: true,
            // quiet: false,
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
