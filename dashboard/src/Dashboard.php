<?php

namespace Dashboard;

use Silex\Application as SilexApplication;
use Silex\Application\UrlGeneratorTrait;
use Silex\Provider\TwigServiceProvider;
use Silex\Provider\UrlGeneratorServiceProvider;

class Dashboard extends SilexApplication
{
	use UrlGeneratorTrait;

	public function __construct()
	{
		parent::__construct();

		$this->register(new TwigServiceProvider(), array('twig.path' => __DIR__.'/../templates'));

		$this['db'] = new \PDO("pgsql:dbname=twitch;host=db", "postgres", "password");
	}
}