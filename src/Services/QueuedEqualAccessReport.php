<?php

namespace App\Services;

use App\Entity\ContentItem;
use Predis\Client as RedisClient;
use DOMDocument;

use Symfony\Component\Console\Output\ConsoleOutput;

// Take in a bundle of ContentItems and
// place them in a Redis queue that 
// Equal Access Server workers will process
class QueuedEqualAccessReport {
    private $client;
    private $redisClient;
    private $scanQueue = 'scan_queue';
    private $resultQueue = 'result_queue';

    public function __construct() {
        $this->loadConfig();
    }

    private function loadConfig() {
        $this->redisClient = new RedisClient([
            'scheme' => 'tcp',
            'host'   => $_ENV['REDIS_HOST'] ?? 'host.docker.internal',
            'port'   => $_ENV['REDIS_PORT'] ?? 6379,
        ]);
    }

    // Source - https://stackoverflow.com/questions/2040240/php-function-to-generate-v4-uuid
    // Posted by Ja͢ck, modified by community. See post 'Timeline' for change history
    // Retrieved 2025-11-12, License - CC BY-SA 4.0
    function uuidv4() {
        $data = random_bytes(16);

        $data[6] = chr(ord($data[6]) & 0x0f | 0x40); // set version to 0100
        $data[8] = chr(ord($data[8]) & 0x3f | 0x80); // set bits 6-7 to 10
            
        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }

    // Send page(s) to the Redis queue
    public function sendToRedis($messageBody) {
        try {
            $this->redisClient->rpush($this->scanQueue, $messageBody);
            return true;
        } catch (\Exception $e) {
            error_log("Error sending message to Redis: " . $e->getMessage());
            return false;
        }
    }

    // After queuing up all the pages we want to scan, we poll the results queue continuously
    // for our scanId bundle until we get all of the reports we need back
    public function pollRedisResults(string $scanId, array $uuids, int $timeoutSeconds = 240): array {
        $output = new ConsoleOutput();
        $results = [];
        $startTime = time();
        $remaining = $uuids;

        $output->writeln("Polling for scanId: " . $scanId);

        while (time() - $startTime < $timeoutSeconds && !empty($remaining)) {
            $foundAny = false;

            foreach ($remaining as $index => $uuid) {
                $key = "result:{$scanId}:{$uuid}";
    
                $reportJson = $this->redisClient->get($key);

                if ($reportJson !== null) {
                    $results[$uuid] = json_decode($reportJson, true);
                    
                    // Remove from the remaining list
                    unset($remaining[$index]);
                    $foundAny = true;
                    
                    // Delete key after consuming
                    $this->redisClient->del($key);
                    
                    $output->writeln(sprintf(
                        "Received %d/%d (UUID: %s)",
                        count($results),
                        count($uuids),
                        substr($uuid, 0, 8)
                    ));
                }
            }

            if ($foundAny) {
                continue;
            }
            
            if (!empty($remaining)) {
                usleep(200000);
            }
        }

        if (!empty($remaining)) {
            $output->writeln("WARNING: Missing " . count($remaining) . " results after timeout");
        }

        return $results;
    }

    // TODO
    // public function postMultipleArrayAsync(array $contentItems): array {
    //     $uuids = [];
    //     $contentItemsReport = [];

    //     // Combine every <num> pages into a request
    //     $htmlArray = [];
    //     $counter = 0;
    //     $payloadSize = 5;
    //     foreach ($contentItems as $contentItem) {
    //         if ($counter >= $payloadSize) {
    //             // Reached our counter limit, create a new payload
    //             // and create and sign a request that we send to the SQS queue
    //             $uuid = $this->uuidv4();
    //             $uuids[] = $uuid;
    //             $payload = json_encode(["uuid" => $uuid, "html" => $htmlArray]);
    //             $this->sendToSqs($payload);
    //             $counter = 0;
    //             $htmlArray = [];
    //         }

    //         // Get the HTML then clean up and push a page into an array
    //         $html = $contentItem->getBody();
    //         $document = $this->getDomDocument($html)->saveHTML();
    //         array_push($htmlArray, $document);

    //         $counter++;
    //     }

    //     // Send out any leftover pages we might have
    //     if (count($htmlArray) > 0) {
    //         $uuid = $this->uuidv4();
    //         $uuids[] = $uuid;
    //         $payload = json_encode(["uuid" => $uuid, "html" => $htmlArray]);

    //         $this->sendToSqs($payload);
    //     }

    //     // Poll the results queue for reports
    //     $results = $this->pollResultsQueue($uuids);

    //     $errors = 0;

    //     foreach ($results as $result) {
    //         // Every "block" of reports pages should be in a stringified
    //         // JSON, so we need to decode the JSON to be able to iterate through
    //         // it first.}

    //         if (isset($result["value"])) {
    //             $response = json_decode($result["value"]->getBody()->getContents(), true);
    //         }
    //         else if (isset($result["reason"])) {
    //             $errors++;
    //         }

    //         foreach ($response as $report) {
    //             $contentItemsReport[] = $report;
    //         }
    //     }

    //     return $contentItemsReport;
    // }

    public function postMultipleAsync(array $contentItems): array {
        $output = new ConsoleOutput();
        $scanId = $this->uuidv4();
        $uuids = [];

        $output->writeln("Starting scan: " . $scanId);

        foreach ($contentItems as $contentItem) {
            $uuid = $this->uuidv4();
            $uuids[] = $uuid;

            $html = $contentItem->getBody();
            $payload = json_encode([
                "scanId" => $scanId,
                "uuid" => $uuid, 
                "html" => $html,
                "guidelineIds" => "WCAG_2_1",
                'reportLevels' => ['violation', 'potentialviolation', 'manual', 'recommendation']
            ]);

            $this->sendToRedis($payload);
        }

        // Poll for results
        $results = $this->pollRedisResults($scanId, $uuids);

        $contentItemsReport = [];
        foreach ($uuids as $uuid) {
            $contentItemsReport[] = $results[$uuid] ?? null;
            if (!isset($results[$uuid])) {
                $output->writeln("Missing result: " . substr($uuid, 0, 8));
            }
        }

        return $contentItemsReport;
    }

    // Scan a single content item
    public function postSingleAsync(ContentItem $contentItem) {
        $scanId = $this->uuidv4();
        $uuid = $this->uuidv4();

        $html = $contentItem->getBody();
        $payload = json_encode([
            "scanId" => $scanId,
            "uuid" => $uuid, 
            "html" => $html,
            "guidelineIds" => "WCAG_2_1",
            'reportLevels' => ['violation', 'potentialviolation', 'manual', 'recommendation']
        ]);

        $this->sendToRedis($payload);
        
        // Poll for results
        $results = $this->pollRedisResults($scanId, [$uuid], 60);

        return $results[$uuid] ?? null;
    }

    public function getDomDocument($html) {
        // TODO: For some reason this causes the scan to not work?
        // Create a clean DOM document
        $dom = new DOMDocument('1.0', 'utf-8');
        $dom->formatOutput = false; // Prevent extra whitespace

        // Suppress libxml errors but store them for debugging
        libxml_use_internal_errors(true);

        // Create a proper HTML5 document with a single wrapper
        $templateHtml = "<!DOCTYPE html>
        <html lang=\"en\">
        <head>
            <meta charset=\"UTF-8\">
            <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
            <title>Placeholder Page Title</title>
        </head>
        <body>
            <main>
                <!-- CONTENT_PLACEHOLDER -->
            </main>
        </body>
        </html>";

        // Insert content properly
        $templateHtml = str_replace('<!-- CONTENT_PLACEHOLDER -->', $html, $templateHtml);

        // Load without flags that might strip attributes
        $dom->loadHTML($templateHtml);

        // Log any parsing errors for debugging
        $errors = libxml_get_errors();
        if (!empty($errors)) {
            $output = new ConsoleOutput();
            $output->writeln("DOM parsing errors: " . count($errors));
            foreach ($errors as $error) {
                $output->writeln("Line {$error->line}: {$error->message}");
            }
        }
        libxml_clear_errors();

        return $dom;
    }
}
