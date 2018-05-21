<?php

namespace Jiyis\Nsq\Queue\Connectors;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Arr;
use Jiyis\Nsq\Adapter\NsqClientManager;
use Jiyis\Nsq\Queue\NsqQueue;

class NsqConnector implements ConnectorInterface
{

    /**
     * Establish a queue connection.
     * @param array $config
     * @return \Illuminate\Contracts\Queue\Queue|NsqQueue
     * @throws \Exception
     */
    public function connect(array $config)
    {
        $client = new NsqClientManager($config);

        return new NsqQueue(
            $client,
            $client->getJob(),
            Arr::get($config, 'retry_delay_time', 60)
        );
    }

}
