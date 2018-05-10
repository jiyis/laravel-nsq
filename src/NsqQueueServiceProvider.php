<?php

namespace Jiyis\Queue;

use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use Jiyis\Nsq\Queue\Connectors\NsqConnector;
use Jiyis\Nsq\Queue\Manager\NsqManager;

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

        $this->app->singleton('nsq', function ($app) {
            $config = $app->make('config')->get('queue.connections.nsq');

            return new NsqManager(array_get($config, 'client', 'c-nsq'), $config);
        });
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
            return new NsqConnector($this->app['nsq']);
        });
    }
}
