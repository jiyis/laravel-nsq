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
     * @var NsqQueue
     */
    protected $nsqQueue;

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
        NsqQueue $nsqQueue,
        $job,
        $queue
    ) {
        $this->container = $container;
        $this->job = $job;
        $this->nsqQueue = $nsqQueue;
        $this->queue = $queue;
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
     * success handle job
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->getCurrentClient()->send(Packet::fin($this->getJobId()));
        $this->getCurrentClient()->send(Packet::rdy(1));
    }


    /**
     * Re-queue a message
     * @param int $delay
     * @return mixed|void
     */
    public function release($delay = 0)
    {
        parent::release($delay);

        $this->getCurrentClient()->send(Packet::req($this->getJobId(), $delay));
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

    /**
     * get nsq swoole client pool
     * @return NsqQueue|string
     */
    public function getQueue()
    {
        return $this->nsqQueue;
    }

    /**
     * get current nsq swoole client
     * @return NsqQueue|string
     */
    public function getCurrentClient()
    {
        return $this->getQueue()->getCurrentClient();
    }

    /**
     * get nsq body message
     * @return mixed
     */
    public function getMessage()
    {
        return Arr::get($this->decoded, 'message');
    }
}
