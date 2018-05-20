<?php

namespace Jiyis\Nsq\Console;

use Illuminate\Queue\Console\WorkCommand as BaseWorkCommand;
use Illuminate\Queue\Worker;
use Illuminate\Support\Facades\Config;

class WorkCommand extends BaseWorkCommand
{

    /**
     * Create a new queue work command.
     *
     * @param  \Illuminate\Queue\Worker $worker
     * @return void
     */
    public function __construct(Worker $worker)
    {
        $this->signature .= "{--job= : The consumer namespace}";
        parent::__construct($worker);
    }


    /**
     * Execute the console command.
     *
     * @return void
     */
    public function fire()
    {
        $this->hasOption('job') && Config::set(['consumer_job' => $this->option('job')]);

        return parent::fire();
    }

}