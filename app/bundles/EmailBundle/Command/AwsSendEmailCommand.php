<?php

namespace Mautic\EmailBundle\Command;

use Aws\CommandInterface;
use Aws\CommandPool;
use Aws\Exception\AwsException;
use Aws\Middleware;
use Aws\ResultInterface;
use Aws\Ses\SesClient;
use GuzzleHttp\Promise\PromiseInterface;
use Mautic\CoreBundle\Command\ModeratedCommand;
use Mautic\Middleware\ConfigAwareTrait;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class AwsSendEmailCommand extends ModeratedCommand
{
    use ConfigAwareTrait;

    private static $ExtMessage = '.message';
    private static $ExtSending = '.sending';
    private static $ExtFailed = '.failed';

    private static $CONCURRENCY = 150;

    private static $Cmd_SendRawEmail = 'SendRawEmail';

    private static $Key_ses_conf = 'ses_conf';
    private static $Key_version = 'version';
    private static $Key_region = 'region';
    private static $Key_credentials = 'credentials';
    private static $Key_key = 'key';
    private static $Key_secret = 'secret';

    protected function configure()
    {
        $this
            ->setName('mautic:email:batch_send')
            ->setDescription('send emails via AWS SES。Note: only process message files located in $mailer_spool_path$/default'); // todo

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

        if (!file_exists($spoolPath)) { // nothing to do
            $this->completeRun();

            return 0;
        }

        try {
            $output->writeln('task start');

            $c = $this->getConf();

            $sesClient = new SesClient($c);

            $commandGenerator = $this->getCommandGenerator($sesClient);

            $pool = new CommandPool($sesClient, $commandGenerator($files), [
                'concurrency' => self::$CONCURRENCY,
                'before' => function (CommandInterface $cmd, $iterKey) {
//                echo "about to send {$iterKey}: " . print_r($cmd->toArray(), true) . "\n";
                },
                'fulfilled' => function (ResultInterface $result, $iterKey, PromiseInterface $aggregatePromise
                ) {
//                echo "completed {$iterKey}: {$result}\n";
                },
                'rejected' => function (AwsException $reason, $iterKey, PromiseInterface $aggregatePromise
                ) {
//                echo "failed {$iterKey}: {$reason}\n";
                }]);

            $promise = $pool->promise();

            $promise->wait();

            $promise->then(function ($value) {
//                $this->output->writeln("the promise was fulfilled with {$value}");
            }, function ($reason) {
//                $this->output->writeln("the promise was rejected with {$reason}");
            });

            $output->writeln('task finished');

            return 0;
        } catch (\Exception $ex) {
            $output->writeln('caught exception: ' . $ex->getMessage());

            $output->writeln('task failed');

            return -1;
        } finally {
            $this->completeRun();
        }
    }

    // check & get ses conf
    private function getConf()
    {
        $c = $this->getConfig();

        if (!array_key_exists(self::$Key_ses_conf, $c)) {
            throw new \Exception('no ses conf');
        }

        $ses = $c[self::$Key_ses_conf];

        if (!array_key_exists(self::$Key_version, $ses) || empty($ses[self::$Key_version])) {
            throw new \Exception('no version in ses conf');
        }

        if (!array_key_exists(self::$Key_region, $ses) || empty($ses[self::$Key_region])) {
            throw new \Exception('no region in ses conf');
        }

        if (!array_key_exists(self::$Key_credentials, $ses)) {
            throw new \Exception('no credentials in ses conf');
        }

        $credentials = $ses[self::$Key_credentials];

        if ((!array_key_exists(self::$Key_key, $credentials) || empty($credentials[self::$Key_key]))
            || (!array_key_exists(self::$Key_secret, $credentials) || empty($credentials[self::$Key_secret]))) {
            throw new \Exception('no key or secret in ses conf');
        }

        return $ses;
    }

    private function getCommandGenerator($sesClient)
    {
        return function (\Iterator $files) use ($sesClient) {
            foreach ($files as $file) {
                if (!$this->endWith($file->getFilename(), self::$ExtMessage)) { // todo handle failed messages
                    continue;
                }

                $filePath = $file->getRealPath();

                $message = unserialize(file_get_contents($filePath));

                // empty file will case type assertion exception
                if (empty($message)) {
                    $this->output->writeln("warning: empty file {$filePath}");

                    continue;
                }

                if (!$reversePath = $this->getReversePath($message)) {
                    continue;
                }

                // try a rename, it's an atomic operation, and avoid locking the file
                if (!rename($filePath, $filePath . self::$ExtSending)) {
                    continue;
                }

                $command = $sesClient->getCommand(self::$Cmd_SendRawEmail, $this->assembleParamOfRawMessage($message));

                $handlerList = $command->getHandlerList();
                $handlerList->appendSign(
                    Middleware::mapResult(function (ResultInterface $result) use ($filePath) {
                        // todo: mark this message to failed if the response is failed

                        unlink($filePath . self::$ExtSending); // remove file

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

    private function assembleParamOfRawMessage(\Swift_Mime_Message $message)
    {
        return [
            'RawMessage' => [
                'Data' => $message->toString(),
            ]
        ];
    }
}
