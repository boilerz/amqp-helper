import logger from '@boilerz/logger';
import ConsumerClient from '../src/ConsumerClient';
import type { Message } from './publisher';

async function singleHandlerConsumerMain() {
  const consumerClient = await ConsumerClient.createAndSetupClient<Message>({
    queueName: 'hello-goodbye-queue',
    async onMessageHandler(message, rootingKey): Promise<void> {
      logger.info({ message, rootingKey }, 'single handler consumer');
    },
  });

  await consumerClient.consume();
}

if (!module.parent) {
  singleHandlerConsumerMain().catch(console.error);
}
