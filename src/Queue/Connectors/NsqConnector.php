<?php

namespace Jiyis\Nsq\Queue\Connectors;

use Enqueue\AmqpTools\DelayStrategyAware;
use Enqueue\AmqpTools\RabbitMqDlxDelayStrategy;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Queue\Events\WorkerStopping;
use Illuminate\Support\Arr;
use Interop\Amqp\AmqpConnectionFactory as InteropAmqpConnectionFactory;
use Interop\Amqp\AmqpConnectionFactory;
use Interop\Amqp\AmqpContext;
use Jiyis\Nsq\Adapter\SwooleNsqClient;
use Jiyis\Nsq\Queue\Manager\NsqManager;
use Jiyis\Nsq\Queue\NsqQueue;

class NsqConnector implements ConnectorInterface
{

    protected $topic;
    protected $channel;
    protected $consumerJob;

    public function __construct()
    {
        $input = new \Symfony\Component\Console\Input\ArgvInput();
        $this->topic = $input->getParameterOption('--topic');
        $this->channel = $input->getParameterOption('--channel');
        $this->consumerJob = $input->getParameterOption('--job');
    }


    /**
     * Establish a queue connection.
     *
     * @param  array $config
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {

        return new NsqQueue(
            new SwooleNsqClient($config, $this->topic, $this->channel),
            $this->consumerJob,
            Arr::get($config, 'retry_delay_time', 60)
        );
    }

    /**
     * Establish a queue connection.
     *
     * @param array $config
     *
     * @return Queue
     */
    public function connectbak(array $config): Queue
    {
        if (false === array_key_exists('factory_class', $config)) {
            throw new \LogicException('The factory_class option is missing though it is required.');
        }

        $factoryClass = $config['factory_class'];
        if (false === class_exists($factoryClass) || false === (new \ReflectionClass($factoryClass))->implementsInterface(InteropAmqpConnectionFactory::class)) {
            throw new \LogicException(sprintf('The factory_class option has to be valid class that implements "%s"', InteropAmqpConnectionFactory::class));
        }

        /** @var AmqpConnectionFactory $factory */
        $factory = new $factoryClass([
            'dsn'            => $config['dsn'],
            'host'           => $config['host'],
            'port'           => $config['port'],
            'user'           => $config['login'],
            'pass'           => $config['password'],
            'vhost'          => $config['vhost'],
            'ssl_on'         => $config['ssl_params']['ssl_on'],
            'ssl_verify'     => $config['ssl_params']['verify_peer'],
            'ssl_cacert'     => $config['ssl_params']['cafile'],
            'ssl_cert'       => $config['ssl_params']['local_cert'],
            'ssl_key'        => $config['ssl_params']['local_key'],
            'ssl_passphrase' => $config['ssl_params']['passphrase'],
        ]);

        if ($factory instanceof DelayStrategyAware) {
            $factory->setDelayStrategy(new RabbitMqDlxDelayStrategy());
        }

        /** @var AmqpContext $context */
        $context = $factory->createContext();

        $this->dispatcher->listen(WorkerStopping::class, function () use ($context) {
            $context->close();
        });

        return new NsqQueue($context, $config);
    }
}
