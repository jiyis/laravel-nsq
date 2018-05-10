<?php

namespace Jiyis\Nsq\Queue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Jiyis\Nsq\Message\Unpack;
use Jiyis\Nsq\Queue\Jobs\NsqJob;
use Jiyis\Nsq\Queue\Manager\NsqManager;

class NsqQueue extends Queue implements QueueContract
{
    /**
     * The nsq factory implementation.
     *
     */
    protected $nsq;

    /**
     * The connection name.
     */
    protected $connection;

    /**
     * The name of the default queue.
     *
     * @var string
     */
    protected $default;

    /**
     * The expiration time of a job.
     *
     * @var int|null
     */
    protected $retryAfter = 60;

    /**
     * NsqQueue constructor.
     * @param NsqManager $nsq
     * @param string $default
     * @param null $connection
     * @param int $retryAfter
     */
    public function __construct(NsqManager $nsq, $default = 'nsq', $connection = null, $retryAfter = 60)
    {
        $this->nsq = $nsq;
        $this->default = $default;
        $this->connection = $connection;
        $this->retryAfter = $retryAfter;
    }

    /** @inheritdoc */
    public function size($queueName = null): int
    {
        /** @var AmqpQueue $queue */
        list($queue) = $this->declareEverything($queueName);

        return $this->context->declareQueue($queue);
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string  $job
     * @param  mixed   $data
     * @param  string  $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($queue, $this->createPayload($job, $data));
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string  $payload
     * @param  string  $queue
     * @param  array   $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->getConnection()->rpush($this->getQueue($queue), $payload);

        return Arr::get(json_decode($payload, true), 'id');
    }

    /** @inheritdoc */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, ['delay' => $this->secondsUntil($delay)]);
    }

    /**
     * Release a reserved job back onto the queue.
     *
     * @param  \DateTimeInterface|\DateInterval|int $delay
     * @param  string|object $job
     * @param  mixed $data
     * @param  string $queue
     * @param  int $attempts
     * @return mixed
     */
    public function release($delay, $job, $data, $queue, $attempts = 0)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, [
            'delay'    => $this->secondsUntil($delay),
            'attempts' => $attempts
        ]);
    }

    /** @inheritdoc */
    public function pop($queue = null)
    {
        try {
            $data = @$this->nsq->recv();
            if($data == false) return null;
            $frame = Unpack::getFrame($data);

            if (Unpack::isHeartbeat($frame)) {
                $this->nsq->send(Packet::nop());
            } elseif (Unpack::isOk($frame)) {
                $this->nsq->send(Packet::rdy(1));
            } elseif (Unpack::isError($frame)) {
                return null;
            } elseif (Unpack::isMessage($frame)) {

                return new NsqJob($this->container, $this, $frame, $this->connectionName);
            }  else {

            }

            // mark as done; get next on the way
           /* $client->send(fin($frame['id']));
            $client->send(rdy(1));*/

        } catch (\Exception $exception) {
            $this->reportConnectionError('pop', $exception);
        }

        return null;
    }

    /**
     * Get the underlying Nsq instance.
     * @return NsqManager
     */
    public function getNsq()
    {
        return $this->nsq;
    }

    /**
     * Get the connection for the queue.
     *
     * @return \Predis\ClientInterface
     */
    protected function getConnection()
    {
        return $this->nsq->connection($this->connection);
    }

    /**
     * @param string $job
     * @param string $data
     * @param null $queue
     * @return string
     */
    protected function createPayload($job, $data = '', $queue = null)
    {
        $payload = json_encode([
            'msg'                   => $data,
            'composite_http_header' => [
                "request_id"         => config('request_id'),
                "authorization"      => config('authorization'),
                "app_key"            => config('app_key'),
                "consumer_tenant_id" => config('tenant_id'),
                "consumer_user_id"   => config('user_id'),
            ]
        ]);

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidPayloadException(
                'Unable to JSON encode payload. Error code: '.json_last_error()
            );
        }

        return $payload;
    }

}
