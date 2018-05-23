<?php

namespace Jiyis\Nsq\Adapter;

use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Log;
use Jiyis\Nsq\Lookup\Lookup;
use Jiyis\Nsq\Monitor\Consumer;
use Jiyis\Nsq\Monitor\Producer;

class NsqClientManager
{

    /**
     * nsq config
     * @var array
     */
    protected $config;

    /**
     * nsq tcp sub client pool
     * @var
     */
    protected $consumerPool = [];

    /**
     * nsq tcp pub client pool
     * @var
     */
    protected $producerPool = [];

    /**
     * nsq tcp pub client
     * @var
     */
    protected $client;

    /**
     * nsq consumer job
     * @var
     */
    protected $consumerJob = null;

    /**
     * nsq topic name
     * @var
     */
    protected $topic = null;

    /**
     * nsq channel name
     * @var
     */
    protected $channel = null;

    /**
     * connect time
     * @var
     */
    protected $connectTime;


    /**
     * NsqClientManager constructor.
     * @param array $config
     * @throws \Exception
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->connect();
    }

    /**
     * reflect job, get topic and channel
     * @throws \ReflectionException
     */
    public function reflectionJob()
    {
        $this->consumerJob = app(Config::get('consumer_job'));
        $reflect = new \ReflectionClass($this->consumerJob);

        if ($reflect->hasProperty('topic') && $reflect->hasProperty('channel')) {
            $this->topic = $reflect->getProperty('topic')->getValue($this->consumerJob);
            $this->channel = $reflect->getProperty('channel')->getValue($this->consumerJob);
        }
    }

    /**
     * Create a new clustered nsq connection.
     * @throws \Exception
     * @return mixed
     */
    public function connect()
    {
        $this->connectTime = time();
        /**
         * if topic and channel is not null, then the command is sub
         */
        if (Config::get('consumer_job')) {
            $this->reflectionJob();

            $lookup = new Lookup(Arr::get($this->config, 'connection.nsqlookup_url', ['127.0.0.1:4161']));
            $nsqdList = $lookup->lookupHosts($this->topic);

            foreach ($nsqdList['lookupHosts'] as $item) {
                $consumer = new Consumer($item, $this->config, $this->topic, $this->channel);

                $this->consumerPool[$item] = $consumer;
            }

        } else {
            /**
             * if topic and channel is null, then the command is pub
             */
            $hosts = Arr::get($this->config, 'connection.nsqd_url', ['127.0.0.1:4150']);
            foreach ($hosts as $item) {
                $producer = new Producer($item, $this->config);
                $this->producerPool[$item] = $producer;
            }
        }


    }

    /**
     * get nsq pub client pool
     * @return mixed
     */
    public function getProducerPool()
    {
        return $this->producerPool;
    }

    /**
     * get nsq sub client pool
     * @return mixed
     */
    public function getConsumerPool()
    {
        return $this->consumerPool;
    }

    /**
     * @param $key
     * @throws \Exception
     */
    public function reconnectConsumerClient($key)
    {
        $this->consumerPool[$key] = new Consumer($key, $this->config, $this->topic, $this->channel);
    }

    /**
     * @param $key
     * @throws \Exception
     */
    public function reconnectProducerClient($key)
    {
        $this->consumerPool[$key] = new Producer($key, $this->config);;
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
     * get nsq channel
     * @return mixed
     */
    public function getChannel()
    {
        return $this->channel;
    }

    /**
     * get nsq job
     * @return mixed
     */
    public function getJob()
    {
        return $this->consumerJob;
    }

    /**
     * get connect time
     * @return int
     */
    public function getConnectTime()
    {
        return $this->connectTime;
    }

    /**
     * set connect time
     * @return int
     */
    public function setConnectTime($time)
    {
        return $this->connectTime = $time;
    }
}
