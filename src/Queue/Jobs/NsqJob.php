<?php

namespace Jiyis\Nsq\Queue\Jobs;

use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Queue\Jobs\JobName;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use Jiyis\Nsq\Queue\NsqQueue;
use Jiyis\Nsq\Message\Packet;

class NsqJob extends Job implements JobContract
{

    /**
     * The nsq queue instance.
     *
     * @var \Illuminate\Queue\RedisQueue
     */
    protected $nsq;

    /**
     * The nsq raw job payload.
     *
     * @var string
     */
    protected $job;


    /**
     * The nsq top and channel name.
     *
     * @var string
     */
    protected $queue;

    /**
     * Same as NsqQueue, used for attempt counts.
     */
    const ATTEMPT_COUNT_HEADERS_KEY = 'attempts_count';

    protected $connection;
    protected $consumer;
    protected $message;
    protected $decoded;


    public function __construct(
        Container $container,
        NsqQueue $nsq,
        $job,
        $queue,
        $connectionName
    ) {
        $this->container = $container;
        $this->job = $job;
        $this->nsq = $nsq;
        $this->queue = $queue;
        $this->connectionName = $connectionName;
        $this->decoded = $this->payload();
    }

    /**
     * Fire the job.
     *
     * @throws Exception
     *
     * @return void
     */
    public function fire()
    {
        try {
            $payload = $this->payload();

            list($class, $method) = JobName::parse($payload['job']);

            with($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
        } catch (Exception $exception) {
            if (
                $this->causedByDeadlock($exception) ||
                Str::contains($exception->getMessage(), ['detected deadlock'])
            ) {
                sleep(2);
                $this->fire();

                return;
            }

            throw $exception;
        }
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return Arr::get($this->decoded, 'attempts');
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->job;
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        //$this->nsq->getClient()->send(Packet::fin($this->payload()['id']));
        //$this->nsq->getClient()->send(Packet::rdy(1));
    }


    /** @inheritdoc */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->delete();

        $body = $this->payload();

        /*
         * Some jobs don't have the command set, so fall back to just sending it the job name string
         */
        if (isset($body['data']['command']) === true) {
            $job = $this->unserialize($body);
        } else {
            $job = $this->getName();
        }

        $data = $body['data'];

        $this->connection->release($delay, $job, $data, $this->getQueue(), $this->attempts() + 1);
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return Arr::get($this->decoded, 'id');
    }


    public function getClient()
    {
        return $this->nsq->getClient();
    }

    public function getMessage()
    {
        return Arr::get($this->decoded, 'message');
    }
}
