import { Options } from 'amqplib';
import _ from 'lodash';
import wait from 'waait';

import logger from '@boilerz/logger';

import Client, { ClientOptions } from './Client';

export type OnMessageHandler<M = any> = (
  message: M,
  rootingKey?: string,
) => Promise<void>;

export type OnMessageHandlerRecord<
  Msg extends Record<string, any>,
  RKey extends keyof any
> = Record<RKey, OnMessageHandler<Msg>>;

export interface ConsumerClientOptions<
  Msg extends Record<string, any>,
  RKey extends keyof any
> extends Partial<ClientOptions> {
  queueName?: string;
  queueOptions?: Options.AssertQueue;
  nAckThrottle?: number;
}

export interface SingleHandlerConsumerClientOptions<
  Msg extends Record<string, any> = any,
  RKey extends keyof any = ''
> extends ConsumerClientOptions<Msg, RKey> {
  onMessageHandler: OnMessageHandler<Msg>;
}

export interface MultiHandlersConsumerClientOptions<
  Msg extends Record<string, any>,
  RKey extends keyof any
> extends ConsumerClientOptions<Msg, RKey> {
  onMessageHandlerByRootingKey: OnMessageHandlerRecord<Msg, RKey>;
}

class ConsumerClient<
  Msg extends Record<string, any> = any,
  RKey extends keyof any = ''
> extends Client {
  private readonly queueOptions: Options.AssertQueue;

  private readonly nAckThrottle: number;

  private queueName: string;

  private readonly onMessageHandler?: OnMessageHandler<Msg>;

  private readonly onMessageHandlerByRootingKey?: OnMessageHandlerRecord<
    Msg,
    RKey
  >;

  constructor(options: SingleHandlerConsumerClientOptions<Msg, RKey>);

  constructor(options: MultiHandlersConsumerClientOptions<Msg, RKey>);

  constructor({
    queueName = '',
    queueOptions = {},
    nAckThrottle = 0,
    ...singleOrMultiOptions
  }:
    | SingleHandlerConsumerClientOptions<Msg, RKey>
    | MultiHandlersConsumerClientOptions<Msg, RKey>) {
    super(
      _.omit(
        singleOrMultiOptions,
        'onMessageHandler',
        'onMessageHandlerByRootingKey',
      ),
    );
    this.queueName = queueName;
    this.queueOptions = queueOptions;
    this.nAckThrottle = nAckThrottle;
    this.onMessageHandler = (singleOrMultiOptions as SingleHandlerConsumerClientOptions<
      Msg,
      RKey
    >).onMessageHandler;
    this.onMessageHandlerByRootingKey = (singleOrMultiOptions as MultiHandlersConsumerClientOptions<
      Msg,
      RKey
    >).onMessageHandlerByRootingKey;
  }

  async setup(): Promise<void> {
    await super.setup();
    const { queue: actualQueueName } = await this.channel.assertQueue(
      this.queueName,
      this.queueOptions,
    );
    this.queueName = actualQueueName;
    logger.info({ actualQueueName }, '[amqp-helper] setup');

    if (this.onMessageHandlerByRootingKey) {
      await Promise.all(
        Object.keys(this.onMessageHandlerByRootingKey).map((routingKey) =>
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
    Msg extends Record<string, any> = any,
    RKey extends keyof any = ''
  >(
    options: SingleHandlerConsumerClientOptions<Msg, RKey>,
  ): Promise<ConsumerClient<Msg, RKey>>;

  static async createAndSetupClient<
    Msg extends Record<string, any> = any,
    RKey extends keyof any = ''
  >(
    options: MultiHandlersConsumerClientOptions<Msg, RKey>,
  ): Promise<ConsumerClient<Msg, RKey>>;

  static async createAndSetupClient<
    Msg extends Record<string, any> = any,
    RKey extends keyof any = ''
  >(
    options:
      | SingleHandlerConsumerClientOptions<Msg, RKey>
      | MultiHandlersConsumerClientOptions<Msg, RKey>,
  ): Promise<ConsumerClient<Msg, RKey>> {
    const client = _.has(options, 'onMessageHandler')
      ? new ConsumerClient(
          options as SingleHandlerConsumerClientOptions<Msg, RKey>,
        )
      : new ConsumerClient(
          options as MultiHandlersConsumerClientOptions<Msg, RKey>,
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
            const handler = this.onMessageHandlerByRootingKey![routingKey];
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
