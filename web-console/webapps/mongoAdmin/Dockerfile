FROM php:8.1-cli-alpine

WORKDIR /opt/mongodb-php-gui
COPY . /opt/mongodb-php-gui

RUN echo "; PHP settings added by MongoDB PHP GUI (MPG)" > /usr/local/etc/php/conf.d/mpg-docker-php.ini \
  && echo "upload_max_filesize = 25M" >> /usr/local/etc/php/conf.d/mpg-docker-php.ini \
  && echo "post_max_size = 25M" >> /usr/local/etc/php/conf.d/mpg-docker-php.ini \
  && apk update && apk add --no-cache --virtual .build-deps autoconf build-base openssl-dev curl \
  && pecl install mongodb-1.13.0 && docker-php-ext-enable mongodb \
  && curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer \
  && apk del .build-deps \
  && composer install \
  && apk add --no-cache openjdk8-jre

EXPOSE 5000
CMD ["php", "-S", "0.0.0.0:5000"]
