<?php

/**
 * This is an example of queue connection configuration.
 * It will be merged into config/queue.php.
 * You need to set proper values in `.env`
 */
return [

    'driver' => 'nsq',

    'queue' => env('API_PREFIX', 'default'),

    'connection'       => [
        'nsqd_url' => array_filter(explode(',', env('NSQSD_URL', '127.0.0.1:9150'))),

        'nsqlookup_url' => array_filter(explode(',', env('NSQLOOKUP_URL', '127.0.0.1:4161'))),
    ],

    'options' => [
        'rdy' => 1,
    ],

    /*
     * Could be one a class that implements \Interop\Amqp\AmqpConnectionFactory for example:
     *  - \EnqueueAmqpExt\AmqpConnectionFactory if you install enqueue/amqp-ext
     *  - \EnqueueAmqpLib\AmqpConnectionFactory if you install enqueue/amqp-lib
     *  - \EnqueueAmqpBunny\AmqpConnectionFactory if you install enqueue/amqp-bunny
     */

    'client' => [
        'options' => [
            'open_length_check'     => true,
            'package_max_length'    => 2048000,
            'package_length_type'   => 'N',
            'package_length_offset' => 0,
            'package_body_offset'   => 4
        ]

    ],
    
    'host' => env('RABBITMQ_HOST', '127.0.0.1'),
    'port' => env('RABBITMQ_PORT', 5672),

    'vhost'    => env('RABBITMQ_VHOST', '/'),
    'login'    => env('RABBITMQ_LOGIN', 'guest'),
    'password' => env('RABBITMQ_PASSWORD', 'guest'),

    'options' => [

        'exchange' => [

            'name'        => env('RABBITMQ_EXCHANGE_NAME'),

            /*
            * Determine if exchange should be created if it does not exist.
            */
            'declare'     => env('RABBITMQ_EXCHANGE_DECLARE', true),

            /*
            * Read more about possible values at https://www.rabbitmq.com/tutorials/amqp-concepts.html
            */
            'passive'     => env('RABBITMQ_EXCHANGE_PASSIVE', false),
            'durable'     => env('RABBITMQ_EXCHANGE_DURABLE', true),
            'auto_delete' => env('RABBITMQ_EXCHANGE_AUTODELETE', false),
            'arguments'   => env('RABBITMQ_EXCHANGE_ARGUMENTS'),
        ],

    ]
];
