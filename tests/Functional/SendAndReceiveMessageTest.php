<?php

namespace Jiyis\Queue\Tests\Functional;

use Enqueue\AmqpLib\AmqpConnectionFactory;
use Illuminate\Container\Container;
use Illuminate\Events\Dispatcher;
use Interop\Amqp\AmqpTopic;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Jiyis\Queue\Queue\Connectors\NsqConnector;
use Jiyis\Queue\Queue\Jobs\NsqJob;
use Jiyis\Queue\Queue\NsqQueue;

/**
 * @group functional
 */
class SendAndReceiveMessageTest extends TestCase
{
    public function test()
    {
        $config = [
            'factory_class' => AmqpConnectionFactory::class,
            'dsn'      => null,
            'host'     => getenv('HOST'),
            'port'     => getenv('PORT'),
            'login'    => 'guest',
            'password' => 'guest',
            'vhost'    => '/',
            'options' => [
                'exchange' => [
                    'name' => null,
                    'declare' => true,
                    'type' => \Interop\Amqp\AmqpTopic::TYPE_DIRECT,
                    'passive' => false,
                    'durable' => true,
                    'auto_delete' => false,
                ],

                'queue' => [
                    'name' => 'default',
                    'declare' => true,
                    'bind' => true,
                    'passive' => false,
                    'durable' => true,
                    'exclusive' => false,
                    'auto_delete' => false,
                    'arguments' => '[]',
                ],
            ],
            'ssl_params' => [
                'ssl_on'        => false,
                'cafile'        => null,
                'local_cert'    => null,
                'local_key'     => null,
                'verify_peer'   => true,
                'passphrase'    => null,
            ]
        ];

        $connector = new NsqConnector(new Dispatcher());
        /** @var NsqQueue $queue */
        $queue = $connector->connect($config);
        $queue->setContainer($this->createDummyContainer());

        // we need it to declare exchange\queue on RabbitMQ side.
        $queue->pushRaw('something');

        $queue->getContext()->purgeQueue($queue->getContext()->createQueue('default'));

        $expectedPayload = __METHOD__.microtime(true);

        $queue->pushRaw($expectedPayload);

        sleep(1);

        $job = $queue->pop();

        $this->assertInstanceOf(NsqJob::class, $job);
        $this->assertSame($expectedPayload, $job->getRawBody());

        $job->delete();
    }

    private function createDummyContainer()
    {
        $container = new Container();
        $container['log'] = new NullLogger();

        return $container;
    }
}
