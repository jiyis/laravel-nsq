<?php

namespace Jiyis\Nsq\Adapter;


class CNsqManager
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
        $nsq = new Nsq();
        $nsqdAddr = array_get($config, 'nsqd_host', ['127.0.0.1:4150']);
        $nsq->connectNsqd($nsqdAddr);
        $this->producers = $nsq;

        return $this;
    }
}