import logger from '@boilerz/logger';
import { Options } from 'amqplib';
import _ from 'lodash';
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
  private readonly queueName: string;

  private readonly onMessageHandler?: OnMessageHandler<Msg>;

  private readonly onMessageHandlerByRootingKey?: OnMessageHandlerRecord<
    Msg,
    RKey
  >;

  constructor(options: SingleHandlerConsumerClientOptions<Msg, RKey>);

  constructor(options: MultiHandlersConsumerClientOptions<Msg, RKey>);

  constructor({
    queueName = '',
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
      {
        durable: true,
      },
    );
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
            await this.onMessageHandlerByRootingKey![routingKey](message);
          }
          this.channel.ack(consumeMessage);
        } catch (err) {
          logger.error(
            { err, routingKey, message },
            '[amqp-helper] consume - Handler failure',
          );
          this.channel.nack(consumeMessage);
        }
      },
      options || { noAck: false },
    );
  }
}

export default ConsumerClient;
