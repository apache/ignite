var gulp = require('gulp');
var sass = require('gulp-sass');

var paths = [
    './public/stylesheets/*.scss'
];

gulp.task('sass', function () {
    return gulp.src(paths)
        .pipe(sass({ outputStyle: 'nested' }).on('error', sass.logError))
        .pipe(gulp.dest('./build/stylesheets'));
});

gulp.task('sass:watch', function () {
    gulp.watch(paths, ['sass']);
});
