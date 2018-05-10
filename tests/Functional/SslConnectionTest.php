<?php

namespace Jiyis\Queue\Tests\Functional;

use Enqueue\AmqpLib\AmqpConnectionFactory;
use Enqueue\AmqpLib\AmqpContext;
use Illuminate\Events\Dispatcher;
use Interop\Amqp\AmqpTopic;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PHPUnit\Framework\TestCase;
use Jiyis\Queue\Queue\Connectors\NsqConnector;
use Jiyis\Queue\Queue\NsqQueue;

/**
 * @group functional
 */
class SslConnectionTest extends TestCase
{
    public function testConnectorEstablishSecureConnectionWithRabbitMQBroker()
    {
        $this->markTestIncomplete();

        $config = [
            'factory_class' => AmqpConnectionFactory::class,
            'dsn'      => null,
            'host'     => getenv('HOST'),
            'port'     => getenv('PORT_SSL'),
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
                'ssl_on'        => true,
                'cafile'        => getenv('RABBITMQ_SSL_CAFILE'),
                'local_cert'    => null,
                'local_key'     => null,
                'verify_peer'   => false,
                'passphrase'    => null,
            ]
        ];

        $connector = new NsqConnector(new Dispatcher());
        /** @var NsqQueue $queue */
        $queue = $connector->connect($config);

        $this->assertInstanceOf(NsqQueue::class, $queue);

        /** @var AmqpContext $context */
        $context = $queue->getContext();
        $this->assertInstanceOf(AmqpContext::class, $context);

        $this->assertInstanceOf(AMQPSSLConnection::class, $context->getLibChannel()->getConnection());
        $this->assertTrue($context->getLibChannel()->getConnection()->isConnected());
    }
}
