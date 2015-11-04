var gulp = require('gulp');
var jade = require('gulp-jade');

var paths = [
    '!./views/error.jade',
    './views/*.jade',
    './views/**/*.jade'
];

gulp.task('jade', function() {
    gulp.src(paths)
        .pipe(jade({}))
        .pipe(gulp.dest('./build'))
});

gulp.task('jade:watch', function () {
    gulp.watch(paths, ['jade']);
});
