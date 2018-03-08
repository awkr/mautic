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
                'concurrency' => 250,
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
                $origin = $file->getRealPath();

                if (!$this->endWith($origin, '.message') && !$this->endWith($origin, '.message.sending')
                    && !$this->endWith($origin, '.message.tryagain') && !$this->endWith($origin, '.message.finalretry')) {
                    continue;
                }

                if ((time() - filectime($origin)) > 2 * 24 * 3600) { // file is old enough to process
                    continue;
                }

                $message = unserialize(file_get_contents($origin));
                if ($message === false || !is_object($message) || get_class($message) !== 'Swift_Message') {
                    continue;
                }

                $clean = str_replace(['.finalretry', '.sending', '.tryagain'], '', $origin);
                $sending = $clean . '.sending';
                if (!rename($origin, $sending)) {
                    continue;
                }

                $command = $sesClient->getCommand('SendRawEmail', [
                    'RawMessage' => [
                        'Data' => $message->toString(),
                    ]
                ]);
                $command->getHandlerList()->appendSign(
                    Middleware::mapResult(function (ResultInterface $result) use ($origin, $clean, $sending) {
                        if ($result->get('@metadata')['statusCode'] == 200) {
                            unlink($sending);
                        } else {
                            if ($this->endWith($origin, '.finalretry')) {
                                unlink($sending);
                            } else if (!$this->endWith($origin, '.tryagain')) {
                                rename($sending, $clean . 'finalretry');
                            } else if (!$this->endWith($origin, '.sending')) {
                                rename($sending, $clean . 'tryagain');
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
