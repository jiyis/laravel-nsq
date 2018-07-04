## Laravel Nsq Client
NSQ client for laravel

## Requirements

| Dependency | Requirement |
| -------- | -------- |
| [PHP](https://secure.php.net/manual/en/install.php) | `>= 7.1.0` |
| [Swoole](https://www.swoole.co.uk/) | `The Newer The Better` `No longer support PHP5 since 2.0.12` |

## Installation
```
pecl install swoole
```
```
composer require jiyis/laravel-nsq
```

## Usage
#### Set env
```
NSQSD_URL=127.0.0.1:4150
NSQLOOKUP_URL=127.0.0.1:4161

# If it is multiple, please separate them with ","
NSQSD_URL=127.0.0.1:4150,127.0.0.1:4250
```
#### Create Job
```
php artisan make:job NsqTestJob
```
you need set two property. `public $topic;` `public $channel;`
```php
class NsqTestJob  implements ShouldQueue
{

    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;
 
    public $topic = 'test';
    
    public $channel = 'web';

    public function handle()
    {
        $client = $this->job->getCurrentClient();
        $payload = json_decode($this->job->getMessage(), true);
        ...
    }
}
```
#### Publish
```php
// the data you want to be publish 
$str = [
    'message' => 'this is a message',
    'user_id' => 1
];
// not supported dispatch
Queue::connection('nsq')->push(new NsqTestJob, $str);
```
#### Subscribe
```
php artisan queue:work nsq --sleep=3 --tries=3 --timeout=500  --job=App\\Jobs\\NsqTestJob  
```

