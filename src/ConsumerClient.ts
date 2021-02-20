import { Options } from 'amqplib';
import _ from 'lodash';
import wait from 'waait';

import logger from '@boilerz/logger';

import Client, { ClientOptions } from './Client';

export type MessageRecord = Record<string, any>;

export type OnMessageHandler<Msg extends MessageRecord> = (
  message: Msg,
  RoutingKey?: string,
) => Promise<void>;

export type OnMessageHandlerRecord<
  Msg extends MessageRecord,
  RKey extends keyof any
> = Record<RKey, OnMessageHandler<Msg>>;

export interface ConsumerClientOptions extends Partial<ClientOptions> {
  queueName?: string;
  queueOptions?: Options.AssertQueue;
  nAckThrottle?: number;
}

export interface SingleHandlerConsumerClientOptions<
  Msg extends MessageRecord = any
> extends ConsumerClientOptions {
  onMessageHandler: OnMessageHandler<Msg>;
}

export interface MultiHandlersConsumerClientOptions<
  Msg extends MessageRecord,
  RKey extends keyof any
> extends ConsumerClientOptions {
  onMessageHandlerByRoutingKey: OnMessageHandlerRecord<Msg, RKey>;
}

class ConsumerClient<
  Msg extends MessageRecord = any,
  RKey extends keyof any = ''
> extends Client {
  private readonly queueOptions: Options.AssertQueue;

  private readonly nAckThrottle: number;

  private queueName: string;

  private readonly onMessageHandler?: OnMessageHandler<Msg>;

  private readonly onMessageHandlerByRoutingKey?: OnMessageHandlerRecord<
    Msg,
    RKey
  >;

  constructor(options: SingleHandlerConsumerClientOptions<Msg>);

  constructor(options: MultiHandlersConsumerClientOptions<Msg, RKey>);

  constructor({
    queueName = '',
    queueOptions = {},
    nAckThrottle = 0,
    ...singleOrMultiOptions
  }:
    | SingleHandlerConsumerClientOptions<Msg>
    | MultiHandlersConsumerClientOptions<Msg, RKey>) {
    super(
      _.omit(
        singleOrMultiOptions,
        'onMessageHandler',
        'onMessageHandlerByRoutingKey',
      ),
    );
    this.queueName = queueName;
    this.queueOptions = queueOptions;
    this.nAckThrottle = nAckThrottle;
    this.onMessageHandler = (singleOrMultiOptions as SingleHandlerConsumerClientOptions<Msg>).onMessageHandler;
    this.onMessageHandlerByRoutingKey = (singleOrMultiOptions as MultiHandlersConsumerClientOptions<
      Msg,
      RKey
    >).onMessageHandlerByRoutingKey;
  }

  async setup(): Promise<void> {
    await super.setup();
    const { queue: actualQueueName } = await this.channel.assertQueue(
      this.queueName,
      this.queueOptions,
    );
    this.queueName = actualQueueName;
    logger.info({ actualQueueName }, '[amqp-helper] setup');

    if (this.onMessageHandlerByRoutingKey) {
      await Promise.all(
        Object.keys(this.onMessageHandlerByRoutingKey).map((routingKey) =>
          this.channel.bindQueue(
            this.queueName,
            this.options.exchangeName,
            routingKey,
          ),
        ),
      );
    } else {
      await this.channel.bindQueue(
        this.queueName,
        this.options.exchangeName,
        '',
      );
    }
  }

  static async createAndSetupClient<
    Message extends MessageRecord = any,
    RoutingKey extends keyof any = ''
  >(
    options: SingleHandlerConsumerClientOptions<Message>,
  ): Promise<ConsumerClient<Message, RoutingKey>>;

  static async createAndSetupClient<
    Message extends MessageRecord = any,
    RoutingKey extends keyof any = ''
  >(
    options: MultiHandlersConsumerClientOptions<Message, RoutingKey>,
  ): Promise<ConsumerClient<Message, RoutingKey>>;

  static async createAndSetupClient<
    Message extends MessageRecord = any,
    RoutingKey extends keyof any = ''
  >(
    options:
      | SingleHandlerConsumerClientOptions<Message>
      | MultiHandlersConsumerClientOptions<Message, RoutingKey>,
  ): Promise<ConsumerClient<Message, RoutingKey> | ConsumerClient<Message>> {
    const client = _.has(options, 'onMessageHandler')
      ? new ConsumerClient(
          options as SingleHandlerConsumerClientOptions<Message>,
        )
      : new ConsumerClient(
          options as MultiHandlersConsumerClientOptions<Message, RoutingKey>,
        );
    await client.setup();

    return client;
  }

  async consume(options?: Options.Consume): Promise<void> {
    await this.channel.consume(
      this.queueName,
      async (consumeMessage) => {
        if (consumeMessage === null) {
          logger.warn('[amqp-helper] consume - Empty message received');
          return;
        }

        const routingKey: RKey = consumeMessage.fields.routingKey as RKey;
        const message: Msg = JSON.parse(
          consumeMessage.content.toString() || '',
        ) as Msg;
        try {
          if (this.onMessageHandler) {
            await this.onMessageHandler(message, routingKey as string);
          } else {
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            const handler = this.onMessageHandlerByRoutingKey![routingKey];
            if (handler) {
              await handler(message);
            } else {
              logger.warn(
                { routingKey },
                '[amqp-helper] consume - No handler found',
              );
            }
          }
          this.channel.ack(consumeMessage);
        } catch (err) {
          logger.error(
            { err, routingKey, message },
            '[amqp-helper] consume - Handler failure',
          );
          await wait(this.nAckThrottle);
          this.channel.nack(consumeMessage);
        }
      },
      { noAck: false, ...options },
    );
  }

  async waitEmptiness(): Promise<void> {
    await wait(100);
    const { messageCount } = await this.channel.checkQueue(this.queueName);
    if (messageCount === 0) return;

    logger.info({ messageCount }, '[amqp-helper] Waiting more ...');
    await wait(100);
    await this.waitEmptiness();
  }
}

export default ConsumerClient;
