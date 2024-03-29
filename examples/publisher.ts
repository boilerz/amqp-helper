import logger from '@boilerz/logger';

import PublisherClient from '../src/PublisherClient';

export type RoutingKey = 'hello' | 'goodbye';

export type Message = {
  name: string;
};

async function publisherMain(): Promise<void> {
  const publisherClient = await PublisherClient.createAndSetupClient<
    Message,
    RoutingKey
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

if (require.main === module) {
  publisherMain().catch((err) => logger.error({ err }, '[publisher]'));
}
