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
use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class SendEmailCommand extends ModeratedCommand
{
    use ConfigAwareTrait;

    private $logger;

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

        $spoolPath = $this->getContainer()->getParameter('mautic.mailer_spool_path') . '/default';
        if (!file_exists($spoolPath)) {
            $this->completeRun();

            return 0;
        }

        $files = new \DirectoryIterator($spoolPath);

        try {
            $this->logger = new Logger('mailSender');

            $formatter = new LineFormatter("%datetime% %level_name% %message% %context% %extra%\n", null, false, true);

            $handler = new StreamHandler('php://stdout');
            $handler->setFormatter($formatter);
            $this->logger->pushHandler($handler);

            $this->logger->info('start');

            $sdk = new Sdk($this->getSesConf());
            $sesClient = $sdk->createSes();

            $generator = $this->commandGenerator($sesClient);

            $sent = 0;

            $pool = new CommandPool($sesClient, $generator($files), [
                'concurrency' => 150,
                'before' => function (CommandInterface $cmd, $iterKey) {
                },
                'fulfilled' => function (ResultInterface $result, $iterKey, PromiseInterface $aggregatePromise) use (&$sent) {
                    ++$sent;
                },
                'rejected' => function (AwsException $reason, $iterKey, PromiseInterface $aggregatePromise
                ) {
                    throw $reason;
                }
            ]);

            $promise = $pool->promise();

            $promise->wait();

            $promise->then(function ($value) { // fulfilled
            }, function ($reason) { // rejected
            });

            $this->logger->info('done', ['sent' => $sent]);

            return 0;
        } catch (\Exception $e) {
            $this->logger->err('exception', ['message' => $e->getMessage()]);

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
                $originPath = $file->getRealPath();

                if (!$this->endWith($originPath, '.message') && !$this->endWith($originPath, '.message.sending')
                    && !$this->endWith($originPath, '.message.tryagain') && !$this->endWith($originPath, '.message.finalretry')) {
                    continue;
                }

                if ((time() - filectime($originPath)) > 2 * 24 * 3600) { // file is old enough to process
                    continue;
                }

                $message = unserialize(file_get_contents($file->getRealPath()));
                if ($message === false || !is_object($message) || get_class($message) !== 'Swift_Message') {
                    continue;
                }

                $cleanPath = str_replace(['.finalretry', '.sending', '.tryagain'], '', $originPath);
                $sending = $cleanPath . '.sending';
                if (!rename($originPath, $sending)) {
                    continue;
                }

                $command = $sesClient->getCommand('SendRawEmail', [
                    'RawMessage' => [
                        'Data' => $message->toString(),
                    ]
                ]);
                $command->getHandlerList()->appendSign(
                    Middleware::mapResult(function (ResultInterface $result) use ($originPath, $cleanPath, $sending) {
                        if ($result->get('@metadata')['statusCode'] == 200) {
                            unlink($sending);
                        } else {
                            if ($this->endWith($originPath, '.finalretry')) {
                                unlink($sending);
                            } else if (!$this->endWith($originPath, '.tryagain')) {
                                rename($sending, $cleanPath . 'finalretry');
                            } else if (!$this->endWith($originPath, '.sending')) {
                                rename($sending, $cleanPath . 'tryagain');
                            }
                        }

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
}
