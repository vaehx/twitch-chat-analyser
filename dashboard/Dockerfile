FROM php:7-apache

RUN apt-get update \
	&& apt-get install -y --no-install-recommends git unzip libpq-dev \
	&& docker-php-ext-install pdo pdo_pgsql

RUN curl --silent --show-error https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer

RUN a2enmod rewrite
