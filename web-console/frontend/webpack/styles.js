

const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const path = require('path');
const sassUrls = /\.url\.scss$/;

module.exports.devProdScss = (sourceMap = true) => {
    const common = [
        {
            loader: 'css-loader',
            options: {
                sourceMap
            }
        },
        {
            loader: 'sass-loader',
            options: {
                sourceMap,
                sassOptions:{
                    includePaths: [ path.join(__dirname, '../') ]
                }
                
            }
        }
    ];

    return [{
        test: sassUrls,
        use: [
            {
                loader: 'file-loader',
                options: {
                    // This solves leading "/" in JIT Angular styles compiler
                    // https://github.com/angular/angular/issues/4974
                    publicPath: (url) => url
                }
            },
            'extract-loader',
            ...common
        ]
    },
    {
        test: /\.scss$/,
        exclude: sassUrls,
        use: [MiniCssExtractPlugin.loader, ...common]
    }];
};
