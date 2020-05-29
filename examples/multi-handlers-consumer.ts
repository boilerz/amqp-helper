import logger from '@boilerz/logger';
import ConsumerClient from '../src/ConsumerClient';
import type { Message, RootingKey } from './publisher';

async function multiHandlerConsumerMain(): Promise<void> {
  const consumerClient = await ConsumerClient.createAndSetupClient<
    Message,
    RootingKey
  >({
    queueName: 'hello-goodbye-queue',
    onMessageHandlerByRootingKey: {
      async hello(message: Message): Promise<void> {
        logger.info({ message }, 'hello');
      },
      async goodbye(message: Message): Promise<void> {
        logger.info({ message }, 'goodbye');
      },
    },
  });

  await consumerClient.consume();
}

if (!module.parent) {
  multiHandlerConsumerMain().catch((err) =>
    logger.error({ err }, '[multi-handlers-consumer]'),
  );
}
