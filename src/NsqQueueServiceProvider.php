<?php

namespace Jiyis\Nsq;

use Illuminate\Queue\QueueManager;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\ServiceProvider;
use Jiyis\Nsq\Console\WorkCommand;
use Jiyis\Nsq\Message\Packet;
use Jiyis\Nsq\Queue\Connectors\NsqConnector;

class NsqQueueServiceProvider extends ServiceProvider
{

    /**
     * Indicates if loading of the provider is deferred.
     *
     * @var bool
     */
    protected $defer = true;


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

        // rebind queue console command
        $this->app->singleton('command.queue.work', function ($app) {
            return new WorkCommand($app['queue.worker']);
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
            return new NsqConnector;
        });

    }

    /**
     * 取得提供者提供的服务
     *
     * @return array
     */
    public function provides()
    {
        return [WorkCommand::class];
    }
}
