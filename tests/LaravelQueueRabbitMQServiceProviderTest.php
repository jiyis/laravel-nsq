<?php

namespace Jiyis\Queue\Tests;

use Illuminate\Container\Container;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Queue\QueueManager;
use Illuminate\Support\ServiceProvider;
use PHPUnit\Framework\TestCase;
use Jiyis\Queue\NsqQueueServiceProvider;
use Jiyis\Queue\Queue\Connectors\NsqConnector;

class LaravelQueueRabbitMQServiceProviderTest extends TestCase
{
    public function testShouldSubClassServiceProviderClass()
    {
        $rc = new \ReflectionClass(NsqQueueServiceProvider::class);

        $this->assertTrue($rc->isSubclassOf(ServiceProvider::class));
    }

    public function testShouldMergeQueueConfigOnRegister()
    {
        $dir = realpath(__DIR__.'/../src');

        //guard
        $this->assertDirectoryExists($dir);

        $providerMock = $this->createPartialMock(NsqQueueServiceProvider::class, ['mergeConfigFrom']);

        $providerMock
            ->expects($this->once())
            ->method('mergeConfigFrom')
            ->with($dir.'/../config/nsq.php', 'queue.connections.rabbitmq')
        ;

        $providerMock->register();
    }

    public function testShouldAddRabbitMQConnectorOnBoot()
    {
        $dispatcherMock = $this->createMock(Dispatcher::class);

        $queueMock = $this->createMock(QueueManager::class);
        $queueMock
            ->expects($this->once())
            ->method('addConnector')
            ->with('rabbitmq', $this->isInstanceOf(\Closure::class))
            ->willReturnCallback(function ($driver, \Closure $resolver) use ($dispatcherMock) {
                $connector = $resolver();

                $this->assertInstanceOf(NsqConnector::class, $connector);
                $this->assertAttributeSame($dispatcherMock, 'dispatcher', $connector);
            })
        ;

        $app = Container::getInstance();
        $app['queue'] = $queueMock;
        $app['events'] = $dispatcherMock;

        $providerMock = new NsqQueueServiceProvider($app);

        $providerMock->boot();
    }
}
