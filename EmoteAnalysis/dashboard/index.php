<?php

require_once __DIR__.'/vendor/autoload.php';

ini_set('display_errors', 1);
error_reporting(E_ALL);

$app = new Dashboard\Dashboard();
$app['debug'] = true;
$app->mount('', new Dashboard\MainController());
$app->run();
