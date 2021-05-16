import ConsumerClient from '../ConsumerClient';
import PublisherClient from '../PublisherClient';

export type RoutingKey = 'hello' | 'goodbye';

export type Message = {
  name: string;
};

describe('Publisher/Consumer integration test', () => {
  let publisherClient: PublisherClient<Message, RoutingKey | ''>;
  let consumerClient: ConsumerClient<Message, any>;
  let handlerSpy: jest.Mock;

  beforeEach((): void => {
    handlerSpy = jest.fn();
  });

  afterEach(async (): Promise<void> => {
    await publisherClient.tearDown();
    await consumerClient.tearDown();
  });

  it('single handler scenario', async () => {
    publisherClient = await PublisherClient.createAndSetupClient<
      Message,
      RoutingKey
    >();
    consumerClient = await ConsumerClient.createAndSetupClient<Message>({
      queueOptions: { exclusive: true },
      async onMessageHandler(message, routingKey): Promise<void> {
        handlerSpy({ message, routingKey }, 'single handler consumer');
      },
    });

    await publisherClient.publish({
      name: 'John Doe',
    });
    await consumerClient.consume();
    await consumerClient.waitEmptiness();

    expect(handlerSpy).toMatchInlineSnapshot(`
      [MockFunction] {
        "calls": Array [
          Array [
            Object {
              "message": Object {
                "name": "John Doe",
              },
              "routingKey": "",
            },
            "single handler consumer",
          ],
        ],
        "results": Array [
          Object {
            "type": "return",
            "value": undefined,
          },
        ],
      }
    `);
  });

  it('multi handler scenario', async () => {
    publisherClient = await PublisherClient.createAndSetupClient<
      Message,
      RoutingKey
    >();
    consumerClient = await ConsumerClient.createAndSetupClient<
      Message,
      RoutingKey
    >({
      queueOptions: { exclusive: true },
      onMessageHandlerByRoutingKey: {
        async hello(message: Message): Promise<void> {
          handlerSpy({ message }, 'hello');
        },
        async goodbye(message: Message): Promise<void> {
          handlerSpy({ message }, 'goodbye');
        },
      },
    });

    await publisherClient.publish(
      {
        name: 'John Doe',
      },
      'hello',
    );
    await publisherClient.publish(
      {
        name: 'Jane Doe',
      },
      'goodbye',
    );
    await consumerClient.consume();
    await consumerClient.waitEmptiness();

    expect(handlerSpy).toMatchInlineSnapshot(`
      [MockFunction] {
        "calls": Array [
          Array [
            Object {
              "message": Object {
                "name": "John Doe",
              },
            },
            "hello",
          ],
          Array [
            Object {
              "message": Object {
                "name": "Jane Doe",
              },
            },
            "goodbye",
          ],
        ],
        "results": Array [
          Object {
            "type": "return",
            "value": undefined,
          },
          Object {
            "type": "return",
            "value": undefined,
          },
        ],
      }
    `);
  });
});
