<?php

namespace Mautic\EmailBundle\Command;

use Aws\CommandInterface;
use Aws\CommandPool;
use Aws\Exception\AwsException;
use Aws\Middleware;
use Aws\ResultInterface;
use Aws\Sdk;
use Aws\Ses\SesClient;
use GuzzleHttp\Promise\PromiseInterface;
use Mautic\CoreBundle\Command\ModeratedCommand;
use Mautic\Middleware\ConfigAwareTrait;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class AwsSendEmailCommand extends ModeratedCommand
{
    use ConfigAwareTrait;

    protected function configure()
    {
        $this
            ->setName('mautic:email:batch_send')
            ->setDescription('send emails via AWS SES');

        parent::configure();
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        if (!$this->checkRunStatus($input, $output)) {
            return 0;
        }

        $output->setVerbosity(OutputInterface::VERBOSITY_DEBUG);

        $container = $this->getContainer();

        $spoolPath = $container->getParameter('mautic.mailer_spool_path') . '/default';
        $files = new \DirectoryIterator($spoolPath);

        if (!file_exists($spoolPath)) {
            $this->completeRun();

            return 0;
        }

        try {
            $output->writeln('start');

            $sdk = new Sdk($this->getSesConf());
            $sesClient = $sdk->createSes();

            $generator = $this->commandGenerator($sesClient);

            $pool = new CommandPool($sesClient, $generator($files), [
                'concurrency' => 150,
                'before' => function (CommandInterface $cmd, $iterKey) {
                },
                'fulfilled' => function (ResultInterface $result, $iterKey, PromiseInterface $aggregatePromise
                ) {
                },
                'rejected' => function (AwsException $reason, $iterKey, PromiseInterface $aggregatePromise
                ) {
                    throw $reason;
                }
            ]);

            $promise = $pool->promise();

            $promise->wait();

            $promise->then(function ($value) {
                $this->output->writeln("the promise was fulfilled with {$value}");
            }, function ($reason) {
                $this->output->writeln("the promise was rejected with {$reason}");
            });

            $output->writeln('done');

            return 0;
        } catch (\Exception $e) {
            $output->writeln("error: {$e->getMessage()}");

            return -1;
        } finally {
            $this->completeRun();
        }
    }

    /**
     * @return mixed
     * @throws \Exception
     */
    private function getSesConf()
    {
        $c = $this->getConfig();

        if (!array_key_exists('ses_conf', $c)) {
            throw new \Exception('no ses conf');
        }

        $ses = $c['ses_conf'];

        if (!array_key_exists('version', $ses) || empty($ses['version'])) {
            throw new \Exception('no version');
        }

        if (!array_key_exists('region', $ses) || empty($ses['region'])) {
            throw new \Exception('no region');
        }

        if (!array_key_exists('credentials', $ses)) {
            throw new \Exception('no credentials');
        }

        $credentials = $ses['credentials'];

        if ((!array_key_exists('key', $credentials) || empty($credentials['key']))
            || (!array_key_exists('secret', $credentials) || empty($credentials['secret']))) {
            throw new \Exception('no key or secret');
        }

        return $ses;
    }

    private function commandGenerator(SesClient $sesClient)
    {
        return function (\Iterator $files) use ($sesClient) {
            foreach ($files as $file) {
                if (!$this->endWith($file->getFilename(), '.message')) {
                    continue;
                }

                $filePath = $file->getRealPath();
                $sendingPath = $filePath . '.sending';

                $message = unserialize(file_get_contents($filePath));

                if (empty($message) || !$reversePath = $this->getReversePath($message) || !rename($filePath, $sendingPath)) {
                    continue;
                }

                $command = $sesClient->getCommand('SendRawEmail', [
                    'RawMessage' => [
                        'Data' => $message->toString(),
                    ]
                ]);
                $command->getHandlerList()->appendSign(
                    Middleware::mapResult(function (ResultInterface $result) use ($sendingPath) {
                        echo $result->toArray();

                        // todo: mark this message to failed if the response is failed

                        unlink($sendingPath); // remove file

                        return $result;
                    })
                );

                yield $command;
            }
        };
    }

    function endWith($string, $test)
    {
        $strlen = strlen($string);
        $testlen = strlen($test);
        if ($testlen > $strlen) return false;
        return substr_compare($string, $test, $strlen - $testlen, $testlen) === 0;
    }

    private function getReversePath(\Swift_Mime_Message $message)
    {
        $return = $message->getReturnPath();
        $sender = $message->getSender();
        $from = $message->getFrom();

        $path = null;

        if (!empty($return)) {
            $path = $return;
        } elseif (!empty($sender)) {
            // don't use array_keys
            reset($sender); // reset Pointer to first pos
            $path = key($sender); // get key
        } elseif (!empty($from)) {
            reset($from);
            $path = key($from);
        }

        return $path;
    }
}
