<?php

namespace Jiyis\Nsq;

use Illuminate\Support\ServiceProvider;
use Jiyis\Nsq\Provider\WorkCommandProvider;
use Jiyis\Nsq\Queue\Connectors\NsqConnector;

class NsqQueueServiceProvider extends ServiceProvider
{

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/nsq.php', 'queue.connections.nsq'
        );


    }

    /**
     * Register the application's event listeners.
     *
     * @return void
     */
    public function boot()
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        $queue->addConnector('nsq', function () {
            return new NsqConnector;
        });
        // add defer provider, rebind work command
        $this->app->addDeferredServices([WorkCommandProvider::class]);

    }

}
