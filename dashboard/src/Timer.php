<?php

namespace Dashboard;

class Timer
{
	private $time = -1;
	public $times = [];

	public function start()
	{
		$this->time = self::getCurrentTimestamp();
	}

	// Returns elapsed time since start in milliseconds or false if timer not started
	public function stop()
	{
		if ($this->time == -1)
			return false;

		$elapsed = self::getCurrentTimestamp() - $this->time;
		$this->time = 0;
		return $elapsed;
	}

	// Equivalent to stopping and immediately restarting the timer.
	// If name given, stores elapsed time under given name.
	// Returns elapsed time in milliseconds or false if timer not started.
	public function mark($name=null)
	{
		if ($this->time == -1)
			return false;
		
		$currentTime = self::getCurrentTimestamp();
		$elapsed = $currentTime - $this->time;
		$this->time = $currentTime;

		if (!is_null($name))
			$this->times[$name] = $elapsed;

		return $elapsed;
	}

	static function getCurrentTimestamp()
	{
		return round(microtime(true) * 1000);
	}
}
