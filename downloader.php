<?php
define("API", "http://173.255.250.230:7001");
define("THREADS", 128);
define("CONCURENCY", 2);
define("TIMEOUT", 30);
define("RETRY_TIMES", 3);
define("CHUNK_SIZE", 10 * 1000 * 1000);
define("STAGE_PENDDING", 0);
define("STAGE_DOWNLOADING", 1);
define("STAGE_DOWNLOADED", 2);
define("OS_TYPE", \DIRECTORY_SEPARATOR === '\\' ? 'windows' : 'linux');

ini_set("memory_limit", "-1");

use \Workerman\Worker;
use \Workerman\Timer;
use \Workerman\Connection\AsyncTcpConnection;

$mirrors = [
    "nicechia.88cdn.cn" => [
        "104.149.128.42",
        "104.149.128.46",
        "172.107.113.102",
    ]
];
// $bytes = 1000 * 1000 * 1000;
// $writtenBytes = 0;
// $fh = fopen("/data/test4.bin", "w");
// while ($writtenBytes < $bytes) {
//     $junk = rand(100000, 999999);
//     $junk = str_repeat($junk, rand(1, 100));
//     $len = strlen($junk);
//     fwrite($fh, $junk);
//     $writtenBytes += $len;
// }

// exit;
// [root@ns519652 data]# md5sum test*
// 8998a57dc551499d06010a7c1754197f  test2.bin
// 1bc07b53f8c8edb3db75b9f51d5c7634  test3.bin
// 9105c1bcbccb3a0f0fce71df91edbf42  test4.bin
// d054726877cee7c9cca9d230b4ec3db2  test.bin
// [


require_once __DIR__ . '/Workerman/Autoloader.php';

Worker::$pidFile = "tempPidFile";
if (\in_array('-q', $argv)) {
    echo "!!! Downloader Can not run in daemon mode\n";
    exit;
}
// e67e0ddd593100494f8c151371c7a459
$task = new Worker('');
// 进程启动时异步建立一个到www.baidu.com连接对象，并发送数据获取数据
$task->onWorkerStart = function ($worker) {
    global $argv;

    @unlink("tempPidFile");

    $worker->jobDone = false;
    $worker->connectionInitor = 0;
    $worker->config = [];
    $worker->liveThreads = 0;
    $worker->downloadSpeed = 0;
    $worker->downloadQueue = [];
    $worker->taskList = [];
    $worker->connectionPool = [];
    $worker->wasteData = 0;

    // $config['whom'] = read('请输入下载者编号');
    // echo "下载者编号: {$config['whom']}\n";

    // $worker->config['take_id'] = 5; 
    $worker->config['take_id'] = $argv[2] ?? read('Type List ID');
    echo "List ID: {$worker->config['take_id']}\n";

    echo "Fetching List\n";
    $rs = getTaskInfo($worker->config['take_id']);
    preg_match_all("/http(.+).plot/i", $rs, $matches);
    // $rs = "http://104.149.128.46/73d3351dda5d550d3c22f881c3b49f53.bin
    // http://104.149.128.46/d86512f3b27963a5316961b795e5ec1d.bin
    // http://104.149.128.46/7c0387ae7ec1cd53099066c459ff78d5.bin
    // http://104.149.128.46/f5b808acc4e105367d64641563419199.bin
    // http://104.149.128.46/7c058a99d3b36a337c28ca15681a1abc.bin
    // http://104.149.128.46/f7fef5b681d397cf3ff1b89276991669.bin
    // http://104.149.128.46/a09224a05d2e0cb8fcb261879c5b2ae3.bin
    // http://104.149.128.46/fb2c06698cb7439a8e2d78612020311a.bin
    // http://104.149.128.46/b2ba0adaf056c8928ebb63b64274bb78.bin
    // http://104.149.128.46/d0bb77cdcc64d956de75ef49a6766c91.bin";
    // preg_match_all("/http(.+).bin/i", $rs, $matches);

    $urls = $matches[0];
    echo "Matched " . count($urls) . " tasks\n";

    foreach ($urls as $url) {
        $worker->downloadQueue[] = $url;
    }

    $worker->config['store_path'] = $argv[3] ?? read('Type Store Path');
    // $worker->config['store_path'] = '/root/download';
    if (substr($worker->config['store_path'], -1) != '/') {
        $worker->config['store_path'] .= '/';
    }
    $leftSpace = disk_free_space($worker->config['store_path']);
    echo "Path: {$worker->config['store_path']}, Unused Space " . formatSize($leftSpace) . " \n";
    // $n++;
    // echo "开始下载第 $n 个任务\n";
    // $url = $urls[$n - 1];
    // echo "$url\n";
    // $fh = fopen("test.bin", "w");
    // $rs = ftruncate($fh, 1000);
    // fseek($fh, 900);
    // fwrite($fh, "asd");
    // fclose($fh);
    // echo "开始下载第 1 个任务\n";

    Timer::add(0.2, function () use ($worker) {
        if ($worker->jobDone) {
            return;
        }
        // echo "检测任务并发数\n";
        $taskConcurency = count($worker->taskList);
        if ($taskConcurency < CONCURENCY) {
            $success = false;

            // echo "任务并发数小于限制 $taskConcurency / " . CONCURENCY . "\n";
            $url = array_shift($worker->downloadQueue);
            if ($url) {
                echo "Start new task $url \n";
                $worker->taskList[$url] = ['chunks' => false];


                echo "Get task info \n";
                $fileInfo = HTTP_HEAD($url);
                $retry_times = 0;
                do {
                    if ($retry_times) {
                        echo "Get info failed, try $retry_times \n";
                    }
                    $retry_times++;
                    if ($fileInfo && isset($fileInfo['content-length'])) {
                        $worker->taskList[$url]['fileSize'] = $fileInfo['content-length'];
                        echo "File Size: " . formatSize($fileInfo['content-length']) . " \n";
                        $leftSpace = disk_free_space($worker->config['store_path']);
                        if ($leftSpace > $fileInfo['content-length']) {
                            echo "Open file and pre-allocate file space \n";
                            $worker->taskList[$url]['fileName'] = basename($url);
                            $chunks = ceil($worker->taskList[$url]['fileSize'] / CHUNK_SIZE);
                            if (file_exists($worker->config['store_path'] . $worker->taskList[$url]['fileName'])) {
                                echo "File exists, skip \n";
                                break;
                            } else {
                                if (file_exists($worker->config['store_path'] . $worker->taskList[$url]['fileName'] . ".linkDownload")) {
                                    echo "Unfinished Download, Loading BitMap \n";
                                    try {
                                        $worker->taskList[$url]['chunks'] = @json_decode(file_get_contents($worker->config['store_path'] . $worker->taskList[$url]['fileName'] . ".linkDownloadInfo"), true);
                                        if (!$worker->taskList[$url]['chunks'] || count($worker->taskList[$url]['chunks']) != $chunks) {
                                            throw new Exception();
                                        }
                                    } catch (\Throwable $th) {
                                        $worker->taskList[$url]['chunks'] = false;
                                        echo "BitMap Error, Re-download \n";
                                    }
                                }
                                $worker->taskList[$url]['fh'] = fopen($worker->config['store_path'] . $worker->taskList[$url]['fileName'] . ".linkDownload", "c+");
                                if (!$worker->taskList[$url]['fh']) {
                                    echo "Open file failed \n";
                                } else {
                                    if (!$worker->taskList[$url]['chunks']) {
                                        if (ftruncate($worker->taskList[$url]['fh'], $worker->taskList[$url]['fileSize'])) {
                                            echo "Generating BitMap \n";
                                            $success = true;
                                            $worker->taskList[$url]['chunks'] = [];
                                            $startPos = 0;
                                            for ($i = 0; $i < $chunks; ++$i) {
                                                if ($startPos + CHUNK_SIZE  >= $worker->taskList[$url]['fileSize']) {
                                                    $range = "{$startPos}-";
                                                } else {
                                                    $endPos =  $startPos + CHUNK_SIZE - 1;
                                                    $range = "$startPos-$endPos";
                                                    $startPos = $startPos + CHUNK_SIZE;
                                                }
                                                $worker->taskList[$url]['chunks'][$range] = STAGE_PENDDING;
                                            }
                                        }
                                    } else {
                                        foreach ($worker->taskList[$url]['chunks'] as $k => $v) {
                                            if ($v == 1) {
                                                $worker->taskList[$url]['chunks'][$k] = 0;
                                            }
                                        }
                                        $success = true;
                                    }
                                }
                            }
                        } else {
                            echo "{$worker->config['store_path']} insuficient space " . formatSize($leftSpace) . " \n";
                        }
                    } else {
                        echo "Get file size error \n";
                    }
                } while ($retry_times < RETRY_TIMES && !$success);

                if (!$success) {
                    unset($worker->taskList[$url]);
                }
            }
        }

        if ($worker->liveThreads < THREADS) {
            foreach ($worker->taskList as $url => $taskInfo) {
                $total = count($worker->taskList[$url]['chunks']);
                $success = 0;
                foreach ($worker->taskList[$url]['chunks'] as $range => $status) {
                    if ($status == STAGE_PENDDING) {
                        // echo "建立线程\n";
                        $worker->liveThreads++;
                        $worker->taskList[$url]['chunks'][$range] = STAGE_DOWNLOADING;

                        // 建立线程
                        $genThread = function () use ($worker, $url, $range) {
                            global $mirrors;
                            $urlInfo = parse_url($url);

                            $port = $urlInfo['port'] ?? 80;
                            $addr = "tcp://{$urlInfo['host']}:$port";

                            foreach ($mirrors as $search => $items) {
                                $addr = str_ireplace($search, $items[rand(0, count($items) - 1)], $addr);
                            }


                            $httpData = "GET {$urlInfo['path']} HTTP/1.1\r\n";
                            $httpData .= "Host: localhost\r\n";
                            $httpData .= "Connection: close\r\n";
                            $httpData .= "Range: bytes=$range\r\n";
                            $httpData .= "\r\n";

                            // var_dump($httpData);


                            list($startPos) = explode("-", $range);
                            $connection_id = $worker->connectionInitor;
                            $worker->connectionInitor++;
                            $worker->connectionPool[$connection_id] = new AsyncTcpConnection($addr);
                            $worker->connectionPool[$connection_id]->id = $connection_id;
                            $worker->connectionPool[$connection_id]->writePos = $startPos;
                            $worker->connectionPool[$connection_id]->writtenSize = 0;
                            $worker->connectionPool[$connection_id]->readHead = false;
                            $worker->connectionPool[$connection_id]->contentLength = -1;
                            $worker->connectionPool[$connection_id]->lastRecvTime = time();
                            $worker->connectionPool[$connection_id]->onConnect = function ($connection) use ($httpData) {
                                $connection->send($httpData);
                            };
                            $worker->connectionPool[$connection_id]->onMessage = function ($connection, $recv_buffer) use ($worker, $url) {
                                $connection->lastRecvTime = time();
                                $bufferSize = strlen($recv_buffer);
                                if (!$connection->readHead) {
                                    $crlf_pos = \strpos($recv_buffer, "\r\n\r\n");
                                    $head_len = $crlf_pos + 4;
                                    $headData = substr($recv_buffer, 0, $crlf_pos);
                                    $headers = parseHeader($headData);

                                    // var_dump($headers);

                                    if ($headers['status'] != '200' && $headers['status'] != '206') {
                                        $connection->close();
                                        return;
                                    }
                                    if (isset($headers['content-length'])) {
                                        $connection->contentLength = $headers['content-length'];
                                    } else {
                                        $connection->close();
                                        return;
                                    }

                                    $recv_buffer = substr($recv_buffer, $head_len);
                                    $bufferSize = strlen($recv_buffer);
                                    $connection->readHead = true;
                                }

                                // if (rand(0, 10000) == 0) {
                                //     $connection->pauseRecv();
                                // }

                                fseek($worker->taskList[$url]['fh'], $connection->writePos);
                                $bytesWritten = fwrite($worker->taskList[$url]['fh'], $recv_buffer);
                                if ($bytesWritten == $bufferSize) {
                                    $connection->writePos += $bufferSize;
                                    $connection->writtenSize += $bufferSize;
                                    $worker->downloadSpeed += $bufferSize;
                                } else {
                                    $connection->contentLength = -1;
                                    echo $worker->config['store_path'] . $worker->taskList[$url]['fileName'] . " IO Error \n";
                                    // fclose($worker->taskList[$url]['fh']);
                                    $connection->close();
                                }
                            };
                            $worker->connectionPool[$connection_id]->onClose = function ($connection) use ($worker, $url, $range) {
                                $worker->liveThreads--;
                                if ($connection->contentLength == $connection->writtenSize) {
                                    $worker->taskList[$url]['chunks'][$range] = STAGE_DOWNLOADED;
                                    file_put_contents($worker->config['store_path'] . $worker->taskList[$url]['fileName'] . ".linkDownloadInfo", json_encode($worker->taskList[$url]['chunks']));
                                } else {
                                    $worker->wasteData += $connection->writtenSize;
                                    $worker->taskList[$url]['chunks'][$range] = STAGE_PENDDING;
                                }
                                unset($worker->connectionPool[$connection->id]);
                            };
                            $worker->connectionPool[$connection_id]->onError = function ($connection, $code, $msg) {
                                echo "Error code:$code msg:$msg\n";
                            };
                            $worker->connectionPool[$connection_id]->connect();
                        };
                        $genThread();
                        // 线程建立完毕

                        if ($worker->liveThreads == THREADS) {
                            break 2;
                        }
                    } else if ($status == STAGE_DOWNLOADED) {
                        $success++;
                    }
                }
                if ($total == $success) {
                    rename($worker->config['store_path'] . $worker->taskList[$url]['fileName'] . ".linkDownload", $worker->config['store_path'] . $worker->taskList[$url]['fileName']);
                    unlink($worker->config['store_path'] . $worker->taskList[$url]['fileName'] . ".linkDownloadInfo");
                    echo $url . " Downloaded \n";
                    unset($worker->taskList[$url]);
                    if (empty($worker->downloadQueue) && empty($worker->taskList)) {
                        $worker->jobDone = true;
                        echo " \n \nAll finished. \n";
                    }
                }
            }
        }
    });

    Timer::add(1, function () use ($worker) {
        if ($worker->jobDone) {
            return;
        }
        $speed = $worker->downloadSpeed;
        $worker->downloadSpeed = 0;
        $threads = $worker->liveThreads;
        // var_dump($worker->connections);
        echo "\rThreads: {$threads} /" . THREADS . " Speed: " . formatSize($speed) . " /s Queue: " . count($worker->downloadQueue) . "[ Waste Data: " . formatSize($worker->wasteData) . " ]";
        $timeout = time() - TIMEOUT;
        foreach ($worker->connectionPool as $connection) {
            if ($connection->lastRecvTime < $timeout) {
                echo "\r{$connection->id} Recv timeout \n";
                $connection->close();
            }
        }
    });
    // for ($i = 0; $i < THREADS; ++$i) {

    // }

    // $store_path = read('请输入存储路径');
    // echo "存储路径: $store_path";
    // // 不支持直接指定http，但是可以用tcp模拟http协议发送数据
    // $connection_to_baidu = new AsyncTcpConnection('tcp://www.baidu.com:80');
    // // 当连接建立成功时，发送http请求数据
    // $connection_to_baidu->onConnect = function ($connection_to_baidu) {
    //     echo "connect success\n";
    //     $connection_to_baidu->send("GET / HTTP/1.1\r\nHost: www.baidu.com\r\nConnection: keep-alive\r\n\r\n");
    // };
    // $connection_to_baidu->onMessage = function ($connection_to_baidu, $http_buffer) {
    //     echo $http_buffer;
    // };
    // $connection_to_baidu->onClose = function ($connection_to_baidu) {
    //     echo "connection closed\n";
    // };
    // $connection_to_baidu->onError = function ($connection_to_baidu, $code, $msg) {
    //     echo "Error code:$code msg:$msg\n";
    // };
    // $connection_to_baidu->connect();
};
$task->onMessage = function ($connection, $request) {
    // $connection->send("this is body");
    // var_dump($connection->worker->taskList);
    $connection->send(var_export($connection->worker->taskList, true));
};
// 运行worker
Worker::runAll();

function read($str = 'Please Enter')
{
    //提示输入
    fwrite(STDOUT, $str . ":");
    //获取用户输入数据
    $result = trim(fgets(STDIN));
    return trim($result);
}

function getTaskInfo($take_id)
{
    return HTTP_GET(API . "/takeUrls?id=$take_id");
}
function HTTP_POST($url, $para)
{
    if (is_array($para)) {
        $para = http_build_query($para);
    }
    $curl = curl_init($url);
    curl_setopt($curl, CURLOPT_TIMEOUT, 5);
    curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false); //SSL证书认证
    curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 0); //严格认证
    curl_setopt($curl, CURLOPT_HEADER, 0); // 过滤HTTP头
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // 显示输出结果
    curl_setopt($curl, CURLOPT_POST, true); // post传输数据
    curl_setopt($curl, CURLOPT_POSTFIELDS, $para); // post传输数据
    $responseText = curl_exec($curl);
    // var_dump( curl_error($curl) );//如果执行curl过程中出现异常，可打开此开关，以便查看异常内容
    // var_dump( $responseText );//如果执行curl过程中出现异常，可打开此开关，以便查看异常内容
    curl_close($curl);
    return $responseText;
}

function HTTP_GET($url)
{
    $curl = curl_init($url);
    curl_setopt($curl, CURLOPT_TIMEOUT, 5);
    curl_setopt($curl, CURLOPT_HEADER, 0); // 过滤HTTP头
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // 显示输出结果
    curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false); //SSL证书认证
    curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 0); //严格认证
    $responseText = curl_exec($curl);
    //var_dump( curl_error($curl) );//如果执行curl过程中出现异常，可打开此开关，以便查看异常内容
    curl_close($curl);
    return $responseText;
}
function HTTP_HEAD($url)
{
    $curl = curl_init($url);
    curl_setopt($curl, CURLOPT_TIMEOUT, 5);
    curl_setopt($curl, CURLOPT_HEADER, true); // 过滤HTTP头
    curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // 显示输出结果
    curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false); //SSL证书认证
    curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 0); //严格认证
    curl_setopt($curl, CURLOPT_NOBODY, true);
    $responseText = curl_exec($curl);
    $info = curl_getinfo($curl);
    //var_dump( curl_error($curl) );//如果执行curl过程中出现异常，可打开此开关，以便查看异常内容
    curl_close($curl);
    $rs = explode("\r\n", $responseText);
    // var_dump($info);
    $headers = [
        'status' => $info['http_code']
    ];
    foreach ($rs as $v) {
        $tmp = explode(":", $v);
        if (count($tmp) == 2) {
            $headers[trim(strtolower($tmp[0]))] = trim($tmp[1]);
        }
    }

    return $headers;
}
function formatSize($size, $precision = 2)
{
    $units = array('B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB');
    $step = 1024;
    $i = 0;
    while (($size / $step) > 0.9) {
        $size = $size / $step;
        $i++;
    }
    return round($size, $precision) . ' ' . $units[$i];
}
function parseHeader($head_buffer)
{
    $headers = [];

    $crlf_pos = \strpos($head_buffer, "\r\n");
    $statusData = substr($head_buffer, 0, $crlf_pos);
    list($headers['http_version'], $headers['status'], $headers['status_text']) = explode(" ", $statusData);
    $head_buffer = substr($head_buffer, $crlf_pos + 2);

    $head_data = \explode("\r\n", $head_buffer);
    foreach ($head_data as $content) {
        if (false !== \strpos($content, ':')) {
            list($key, $value) = \explode(':', $content, 2);
            $key = \strtolower($key);
            $value = \ltrim($value);
        } else {
            $key = \strtolower($content);
            $value = '';
        }
        if (isset($headers[$key])) {
            $headers[$key] = "{$headers[$key]},$value";
        } else {
            $headers[$key] = $value;
        }
    }
    return $headers;
}
