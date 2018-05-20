<?php

namespace Jiyis\Nsq\Monitor;


abstract class AbstractMonitor
{

    protected $client;

    /**
     * @return mixed
     */
    abstract public function connect();


    /**
     * send data to nsq server
     * @param $data
     * @return mixed
     */
    public function send($data)
    {
        return $this->client->send($data);
    }

    /**
     * get nsq tcp client
     * @return mixed
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * receive data from nsq server
     * @return mixed
     */
    public function receive()
    {
        return @$this->client->recv();
    }

    /**
     * get nsq topic
     * @return mixed
     */
    public function getTopic()
    {
        return $this->topic;
    }

    /**
     * check is connect
     * @return mixed
     */
    public function isConnected()
    {
        return $this->client->isConnected();
    }

    /**
     * @return mixed
     */
    public function reconnect()
    {
        if ($this->client) {
            $this->client->close();
        }

        return $this->connect();
    }

    /**
     * @return mixed
     */
    public function close()
    {
        return $this->client->close();
    }

}