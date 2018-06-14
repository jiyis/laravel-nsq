<?php

/**
 * This is an example of queue connection configuration.
 * It will be merged into config/queue.php.
 * You need to set proper values in `.env`
 */
return [

    'driver' => 'nsq',


    'queue' => env('API_PREFIX', 'default'),

    /*
     |--------------------------------------------------------------------------
     | Nsqd host  nsqlookup host
     |--------------------------------------------------------------------------
     |
     */
    'connection'       => [
        'nsqd_url' => array_filter(explode(',', env('NSQSD_URL', '127.0.0.1:9150'))),

        'nsqlookup_url' => array_filter(explode(',', env('NSQLOOKUP_URL', '127.0.0.1:4161'))),
    ],

    /*
      |--------------------------------------------------------------------------
      | Nsq Config
      |--------------------------------------------------------------------------
      |
      */
    'options' => [
        //Update RDY state (indicate you are ready to receive N messages)
        'rdy' => 1,
        'cl'  => 1
    ],

    /*
      |--------------------------------------------------------------------------
      | Nsq identify
      |--------------------------------------------------------------------------
      |
      */
    'identify' => [
        'user_agent' => 'nsq-client',
    ],


    /*
      |--------------------------------------------------------------------------
      | Swoole Client Params
      |--------------------------------------------------------------------------
      |
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
];
