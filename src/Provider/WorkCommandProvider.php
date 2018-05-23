<?php

namespace Jiyis\Nsq\Provider;


use Illuminate\Support\ServiceProvider;
use Jiyis\Nsq\Console\WorkCommand;

class WorkCommandProvider extends ServiceProvider
{

    protected $defer = true;

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        // rebind queue console command
        $this->app->singleton('command.queue.work', function ($app) {
            return new WorkCommand($app['queue.worker']);
        });

    }

    /**
     * @return array
     */
    public function provides()
    {
        return ['command.queue.work'];
    }

}