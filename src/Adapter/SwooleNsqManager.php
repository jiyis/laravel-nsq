<?php
/**
 * Created by PhpStorm.
 * User: Gary.F.Dong
 * Date: 18-5-10
 * Time: 下午2:46
 * Desc:
 */

namespace Jiyis\Nsq\Adapter;


use Jiyis\Nsq\Message\Packet;
use Swoole\Client;

class SwooleNsqManager
{
    protected $producers;

    protected $consumers;


    /**
     * Create a new clustered nsq connection.
     *
     * @param  array  $config
     * @param  array  $options
     * @return mixed
     */
    public function connect(array $config, array $options)
    {
        $client = new Client(SWOOLE_SOCK_TCP);

        $client->connect('127.0.0.1', 4150, 0.5);
        //连接到服务器
        if (!$client->connect('127.0.0.1', 4150, 0.5))
        {
           throw new \Exception('connect failed.');
        }

        if (!isset($options['topic']) || !isset($options['channel'])) {
            throw new \Exception('Cannot subscribe without topic or channel');
        }

        //向服务器发送数据
        $client->send(Packet::magic());
        $client->send(Packet::sub('test', 'web'));

        $client->send(Packet::rdy(1));

    }
}