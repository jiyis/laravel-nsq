<?php

namespace Jiyis\Nsq\Lookup;

use Illuminate\Support\Facades\Log;
use Jiyis\Nsq\Exception\LookupException;

class Lookup
{
    /**
     * Hosts to connect to
     *
     * @var array
     */
    private $hosts;

    /**
     * Connection timeout, in seconds
     *
     * @var float
     */
    private $connectionTimeout;

    /**
     * Response timeout, in seconds
     *
     * @var float
     */
    private $responseTimeout;

    /**
     * Constructor
     *
     * @param array $hosts Will default to localhost
     * @param float $connectionTimeout
     * @param float $responseTimeout
     */
    public function __construct(array $hosts = [], $connectionTimeout = 1.0, $responseTimeout = 2.0)
    {
        if (empty($hosts)) {
            $this->hosts = [
                ['host' => 'localhost', 'port' => 4161]
            ];
        } else {
            $this->hosts = $hosts;
        }

        $this->connectionTimeout = $connectionTimeout;
        $this->responseTimeout = $responseTimeout;
    }

    /**
     * lookup hosts for a given topic
     * @param string $topic
     * @return array
     */
    public function lookupHosts(string $topic)
    {
        $lookupHosts = [];
        $topicChannel = [];

        foreach ($this->hosts as $item) {
            $url = sprintf('http://%s/lookup?topic=%s', $item, urlencode($topic));

            $ch = curl_init($url);
            $options = [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_HEADER         => false,
                CURLOPT_FOLLOWLOCATION => false,
                CURLOPT_ENCODING       => '',
                CURLOPT_USERAGENT      => 'nsq swoole client',
                CURLOPT_CONNECTTIMEOUT => $this->connectionTimeout,
                CURLOPT_TIMEOUT        => $this->responseTimeout,
                CURLOPT_FAILONERROR    => true
            ];

            curl_setopt_array($ch, $options);
            if (!$resultStr = curl_exec($ch)) {
                throw new LookupException('Error talking to nsq lookupd via ' . $url);
            }

            if (!curl_error($ch) && curl_getinfo($ch, CURLINFO_HTTP_CODE) == '200') {
                $result = json_decode($resultStr, true);
                $producers = [];
                if (isset($result['data']['producers'])) {
                    //0.3.8
                    $producers = $result['data']['producers'];
                } elseif (isset($result['producers'])) {
                    //>=1.0.0
                    $producers = $result['producers'];
                }
                foreach ($producers as $prod) {
                    $address = $prod['broadcast_address'];
                    $host = "{$address}:{$prod['tcp_port']}";
                    if (!in_array($host, $lookupHosts)) {
                        $lookupHosts[] = $host;
                        $topicChannel[$host]['channels'] = $result['data']['channels'] ?? $result['channels'];
                    }
                }
                curl_close($ch);
            } else {
                $err = curl_error($ch);
                Log::error($err . $resultStr);
                curl_close($ch);
                throw new LookupException($err, -1);
            }

        }

        return [
            'lookupHosts'  => $lookupHosts,
            'topicChannel' => $topicChannel
        ];
    }
}