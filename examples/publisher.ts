import logger from '@boilerz/logger';
import PublisherClient from '../src/PublisherClient';

export type RootingKey = 'hello' | 'goodbye';

export type Message = {
  name: string;
};

async function publisherMain(): Promise<void> {
  const publisherClient = await PublisherClient.createAndSetupClient<
    Message,
    RootingKey
  >();

  await publisherClient.publish(
    {
      name: 'John',
    },
    'hello',
  );
  await publisherClient.publish(
    {
      name: 'John',
    },
    'goodbye',
  );
}

if (!module.parent) {
  publisherMain().catch((err) => logger.error({ err }, '[publisher]'));
}
