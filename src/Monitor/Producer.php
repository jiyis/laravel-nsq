<?php

namespace Jiyis\Nsq\Monitor;

use Illuminate\Support\Arr;
use Jiyis\Nsq\Message\Packet;
use Swoole\Client;

class Producer extends AbstractMonitor
{

    /**
     * Nsqd config
     *
     * @var string
     */
    protected $config;

    /**
     * Nsqd host
     *
     * @var string
     */
    protected $host;

    public function __construct($host, array $config)
    {
        $this->host = $host;
        $this->config = $config;
        $this->connect();

    }

    public function connect()
    {
        // init swoole client
        $this->client = new Client(SWOOLE_SOCK_TCP, SWOOLE_SOCK_SYNC);

        // set swoole tcp client config
        $this->client->set(Arr::get($this->config, 'client.options'));

        list($host, $port) = explode(':', $this->host);
        // connect nsq server
        if (!$this->client->connect($host, $port, 3)) {
            throw new \Exception('connect nsq server failed.');
        }

        $this->client->send(Packet::magic());

        $this->client->send(Packet::identify([
            'client_id'           => $this->host,
            'hostname'            => gethostname(),
            'user_agent'          => 'nsq_swoole_client_pub',
            'heartbeat_interval'  => -1,
            'feature_negotiation' => true
        ]));

        $this->client->recv();

    }

}