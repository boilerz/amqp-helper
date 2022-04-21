import logger from '@boilerz/logger';

import ConsumerClient from '../src/ConsumerClient';
import type { Message, RoutingKey } from './publisher';

async function multiHandlerConsumerMain(): Promise<void> {
  const consumerClient = await ConsumerClient.createAndSetupClient<
    Message,
    RoutingKey
  >({
    queueName: 'hello-goodbye-queue',
    onMessageHandlerByRoutingKey: {
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

if (require.main === module) {
  multiHandlerConsumerMain().catch((err) =>
    logger.error({ err }, '[multi-handlers-consumer]'),
  );
}
