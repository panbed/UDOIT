<?php

namespace App\Services;

use App\Entity\ContentItem;

use DOMDocument;

use Aws\Credentials\Credentials;
use Aws\Signature\SignatureV4;

use Symfony\Component\Console\Output\ConsoleOutput;

// Take in a bundle of ContentItems and
// send them to an SQS queue for processing
// by Equal Access Server workers

class QueuedEqualAccessReport {
    private $client;
    private $awsAccessKeyId;
    private $awsSecretAccessKey;
    private $awsRegion;
    private $sqsScanQueue;
    private $sqsResultQueue;

    public function __construct() {
        $this->loadConfig();
    }

    private function loadConfig() {
        // Load variables for AWS
        $this->awsAccessKeyId = $_ENV['EQUALACCESS_AWS_ACCESS_KEY_ID'];
        $this->awsSecretAccessKey = $_ENV['EQUALACCESS_AWS_SECRET_ACCESS_KEY'];
        $this->awsRegion = $_ENV['EQUALACCESS_AWS_REGION'];
        $this->sqsScanQueue = $_ENV['EQUALACCESS_AWS_SQS_SCAN_QUEUE_URL'];
        $this->sqsResultQueue = $_ENV['EQUALACCESS_AWS_SQS_RESULT_QUEUE_URL'];
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

    public function sendToSqs($messageBody, $scanId) {
        $sqsClient = new \Aws\Sqs\SqsClient([
            'region' => $this->awsRegion,
            'version' => 'latest',
            'credentials' => [
                'key' => $this->awsAccessKeyId,
                'secret' => $this->awsSecretAccessKey,
            ],
            'http' => [
                'connect_timeout' => 5,
                'timeout' => 240,
            ]
        ]);

        // Send the page to the SQS scan queue (currently a FIFO queue)
        try {
            $result = $sqsClient->sendMessage([
                'QueueUrl' => $this->sqsScanQueue,
                'MessageBody' => $messageBody,
                'MessageGroupId' => "scan-{$scanId}",
                'MessageDeduplicationId' => $this->uuidv4(),
            ]);

            return $result->get('MessageId');
        } catch (\Aws\Exception\AwsException $e) {
            $output = new ConsoleOutput();
            $output->writeln('Error sending message to SQS: ' . $e->getMessage());
            return null;
        }
    }

    // After queuing up all the pages we want to scan, we poll the results queue continuously
    // with the scanId we sent earlier
    public function pollResultsQueue($scanId, array $uuids, int $timeoutSeconds = 240, int $pollIntervalSeconds = 5): array {
        $output = new ConsoleOutput();
        $sqsClient = new \Aws\Sqs\SqsClient([
            'region' => $this->awsRegion,
            'version' => 'latest',
            'credentials' => [
                'key' => $this->awsAccessKeyId,
                'secret' => $this->awsSecretAccessKey,
            ],
        ]);

        $results = [];
        $startTime = time();
        $count = 0;

        // Loop until we reach the timeout (default 4 minutes, 240 seconds) or
        // until we have the same number of UUIDs (page scans) as results
        while (time() - $startTime < $timeoutSeconds && count($results) < count($uuids)) {
            $result = $sqsClient->receiveMessage([
                'QueueUrl' => $this->sqsResultQueue,
                'MaxNumberOfMessages' => 10,
                'WaitTimeSeconds' => 20,
            ]);

            if (!empty($result->get('Messages'))) {
                foreach ($result->get('Messages') as $message) {
                    $body = json_decode($message['Body'], true);

                    // Check if the message belongs to our scan by checking both
                    // the scanId (the entire scan) and the UUIDs (each individual page)
                    if ($body['scanId'] === $scanId && in_array($body['uuid'], $uuids)) {
                        $results[$body['uuid']] = $body['report'];
                        $count++;

                        // Delete the message from the queue
                        $sqsClient->deleteMessage([
                            'QueueUrl' => $this->sqsResultQueue,
                            'ReceiptHandle' => $message['ReceiptHandle'],
                        ]);
                    }
                }
            }

            // sleep($pollIntervalSeconds);
        }

        // $output = new ConsoleOutput();
        // $output->writeln(json_encode($results, JSON_PRETTY_PRINT));
        // $output->writeln("Received {count($results)} out of " . count($uuids) . " results for scan {$scanId}");

        return $results;
    }

    public function postMultipleArrayAsync(array $contentItems): array {
        // TODO: Update for FIFO queue
        $uuids = [];
        $contentItemsReport = [];

        // Combine every <num> pages into a request
        $htmlArray = [];
        $counter = 0;
        $payloadSize = 5;
        foreach ($contentItems as $contentItem) {
            if ($counter >= $payloadSize) {
                // Reached our counter limit, create a new payload
                // and create and sign a request that we send to the SQS queue
                $uuid = $this->uuidv4();
                $uuids[] = $uuid;
                $payload = json_encode(["uuid" => $uuid, "html" => $htmlArray]);
                $this->sendToSqs($payload);
                $counter = 0;
                $htmlArray = [];
            }

            // Get the HTML then clean up and push a page into an array
            $html = $contentItem->getBody();
            $document = $this->getDomDocument($html)->saveHTML();
            array_push($htmlArray, $document);

            $counter++;
        }

        // Send out any leftover pages we might have
        if (count($htmlArray) > 0) {
            $uuid = $this->uuidv4();
            $uuids[] = $uuid;
            $payload = json_encode(["uuid" => $uuid, "html" => $htmlArray]);

            $this->sendToSqs($payload);
        }

        // Poll the results queue for reports
        $results = $this->pollResultsQueue($uuids);

        $output = new ConsoleOutput();
        // $output->writeln(json_encode($results, JSON_PRETTY_PRINT));

        $errors = 0;

        foreach ($results as $result) {
            // Every "block" of reports pages should be in a stringified
            // JSON, so we need to decode the JSON to be able to iterate through
            // it first.}

            if (isset($result["value"])) {
                $response = json_decode($result["value"]->getBody()->getContents(), true);
            }
            else if (isset($result["reason"])) {
                $errors++;
            }

            foreach ($response as $report) {
                $contentItemsReport[] = $report;
            }
        }

        return $contentItemsReport;
    }


    public function postMultipleAsync(array $contentItems): array {
        $output = new ConsoleOutput();

        $scanId = $this->uuidv4();
        $uuids = [];

        $output->writeln("Starting scan with ID: {$scanId}");

        // Queue up all content items to be scanned
        foreach ($contentItems as $contentItem) {
            $uuid = $this->uuidv4();
            $uuids[] = $uuid;

            // Get the HTML, clean it up, save it into a JSON and then queue it up
            $html = $contentItem->getBody();
            // $document = $this->getDomDocument($html)->saveHTML();
            $payload = json_encode([
                "scanId" => $scanId,
                "uuid" => $uuid, 
                "html" => $html,
                "guidelineIds" => "WCAG_2_1",
                'reportLevels' => ['violation', 'potentialviolation', 'manual', 'recommendation']
            ]);

            $this->sendToSqs($payload, $scanId);
        }

        // Poll for results from the results queue
        $results = $this->pollResultsQueue($scanId, $uuids);

        // Save the report for the content item into an array.
        // They should (in theory) be in the same order they were sent in.
        $contentItemsReport = [];

        foreach ($uuids as $uuid) {
            if (isset($results[$uuid])) {
                $contentItemsReport[] = $results[$uuid];
            }
            else {
                $output->writeln("No result for UUID: {$uuid}");
            }
        }

        // $output->writeln(json_encode($results, JSON_PRETTY_PRINT));
        
        // foreach ($results as $uuid => $report) {
        //     // $response = $result->getBody()->getContents();
        //     // $json = json_decode($response, true);

        //     // $output->writeln(json_encode($report, JSON_PRETTY_PRINT));

        //     $contentItemsReport[] = $report;
        // }

        return $contentItemsReport;
    }

    // Scan a single content item
    public function postSingleAsync(ContentItem $contentItem) {
        $scanId = $this->uuidv4();
        $uuid = $this->uuidv4();

        // Clean up the content item's HTML document and create a payload to send
        $html = $contentItem->getBody();
        // $document = $this->getDomDocument($html)->saveHTML();
        $payload = json_encode([
            "scanId" => $scanId,
            "uuid" => $uuid, 
            "html" => $html,
            "guidelineIds" => "WCAG_2_1",
            'reportLevels' => ['violation', 'potentialviolation', 'manual', 'recommendation']
        ]);

        $this->sendToSqs($payload, $scanId);

        // Poll for the result from the results queue
        $report = $this->pollResultsQueue($scanId, [$uuid], 60);

        // $output = new ConsoleOutput();
        // $output->writeln("Single async result:");
        // $output->writeln(json_encode($report, JSON_PRETTY_PRINT));

        // Return the Equal Access report
        return $report[$uuid] ?? null;
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
