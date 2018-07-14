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

		$this['twig']->addFilter(new \Twig_SimpleFilter('date_duration', function($durationMS) {
			$a = new \DateTime();
			$a->setTimestamp(floor($durationMS / 1000));
			
			$b = new \DateTime();
			$b->setTimestamp(0);

			$d = $a->diff($b);
			
			$formatted = ''
				. ($d->y > 0 ? $d->y . 'Y ' : '')
				. ($d->m > 0 ? $d->m . 'M ' : '')
				. ($d->d > 0 ? $d->d . 'd ' : '')
				. ($d->h > 0 ? $d->h . 'h ' : '')
				. ($d->i > 0 ? $d->i . 'min ' : '')
				. ($d->s > 0 ? $d->s . 's ' : '');
			return rtrim($formatted);
		}));

		$this['db'] = new \PDO("pgsql:dbname=twitch;host=db", "postgres", "password");
	}
}