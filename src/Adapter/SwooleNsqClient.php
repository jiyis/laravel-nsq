<?php

namespace Jiyis\Nsq\Adapter;

use Jiyis\Nsq\Message\Packet;
use Swoole\Client;

class SwooleNsqClient
{
    protected $producers;

    protected $consumers;

    protected $config;

    protected $client;

    protected $topic;
    protected $channel;


    public function __construct(array $config, string $topic, string $channel)
    {
        $this->config = $config;
        $this->topic = $topic;
        $this->channel = $channel;
        $this->connect();
    }

    /**
     * Create a new clustered nsq connection.
     *
     * @param  array $config
     * @param  array $options
     * @return mixed
     */
    public function connect()
    {
        $this->client = new Client(SWOOLE_SOCK_TCP);
        $this->client->set([
            'open_length_check'     => true,
            'package_max_length'    => 2048000,
            'package_length_type'   => 'N',
            'package_length_offset' => 0,
            'package_body_offset'   => 4

        ]);

        //连接到服务器
        if (!$this->client->connect('127.0.0.1', 4150, 0.5)) {
            throw new \Exception('connect failed.');
        }

        /*if (!isset($options['topic']) || !isset($options['channel'])) {
            throw new \Exception('Cannot subscribe without topic or channel');
        }*/

        //向服务器发送数据
        $this->client->send(Packet::magic());
        $this->client->send(Packet::sub($this->topic, $this->channel));

        $this->client->send(Packet::rdy(1));

    }

    public function send($data)
    {
        return $this->client->send($data);
    }

    public function getClient()
    {
        return $this->client;
    }

    public function receive()
    {
        return $this->client->recv();
    }
}