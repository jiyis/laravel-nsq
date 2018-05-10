<?php

namespace Jiyis\Nsq\Queue\Manager;


use Illuminate\Support\Arr;
use Jiyis\Nsq\Adapter\CNsqManager;
use Jiyis\Nsq\Adapter\SwooleNsqManager;

class NsqManager
{

    /**
     * The name of the default driver.
     *
     * @var string
     */
    protected $driver;

    /**
     * The Redis server configurations.
     *
     * @var array
     */
    protected $config;

    /**
     * The Redis connections.
     *
     * @var mixed
     */
    protected $connections;


    /**
     * NsqAdapter constructor.
     * @param $driver
     * @param array $config
     */
    public function __construct($driver, array $config)
    {
        $this->driver = $driver;
        $this->config = $config;
    }

    /**
     * Get a Nsq connection by name.
     *
     * @param  string|null  $name
     * @return \Illuminate\Redis\Connections\Connection
     */
    public function connection($name = null)
    {
        $name = $name ?: 'default';

        if (isset($this->connections[$name])) {
            return $this->connections[$name];
        }

        return $this->connections[$name] = $this->resolve($name);
    }

    /**
     * Resolve the given connection by name.
     * @param null $name
     * @return mixed
     */
    public function resolve($name = null)
    {
        $name = $name ?: 'default';

        $options = Arr::get($this->config, 'options', []);

        if (isset($this->config[$name])) {
            return $this->connector()->connect($this->config[$name], $options);
        }


        throw new InvalidArgumentException(
            "Redis connection [{$name}] not configured."
        );
    }

    /**
     * Get the connector instance for the current driver.
     *
     * @return mixed
     */
    protected function connector()
    {
        return new SwooleNsqManager();
        switch ($this->driver) {
            case 'c-nsq':
                return new CNsqManager();
            case 'swoole-nsq':
                return new SwooleNsqManager();
        }
    }

    /**
     * Pass methods onto the default Redis connection.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->connection()->{$method}(...$parameters);
    }
}
