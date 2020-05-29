import PublisherClient from '../src/PublisherClient';

export type RootingKey = 'hello' | 'goodbye';

export type Message = {
  name: string;
};

async function publisherMain() {
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
  publisherMain().catch(console.error);
}
