import { Options } from 'amqplib';

import Client, { ClientOptions } from './Client';

export interface PublishOptions extends Options.Publish {
  routingKey?: string;
}

class PublisherClient<
  Msg extends Record<string, any>,
  RKey extends keyof any = ''
> extends Client {
  constructor(options: Partial<ClientOptions> = {}) {
    super(options);
  }

  static async createAndSetupClient<
    Message extends Record<string, any>,
    RoutingKey extends keyof any = ''
  >(
    options: Partial<ClientOptions> = {},
  ): Promise<PublisherClient<Message, RoutingKey>> {
    const client = new PublisherClient<Message, RoutingKey>(options);
    await client.setup();

    return client;
  }

  async publish(message: Msg): Promise<void>;

  async publish(message: Msg, routingKey: RKey): Promise<void>;

  async publish(message: Msg, options: PublishOptions): Promise<void>;

  async publish(
    message: Msg,
    routingKeyOrOptions?: RKey | PublishOptions,
  ): Promise<void> {
    let publishOptions: Options.Publish = { persistent: true };
    let routingKey = '';
    if (routingKeyOrOptions) {
      const routingKeyProvided = typeof routingKeyOrOptions === 'string';
      routingKey = routingKeyProvided
        ? (routingKeyOrOptions as string)
        : (routingKeyOrOptions as PublishOptions).routingKey || '';
      publishOptions = routingKeyProvided
        ? { persistent: true }
        : (routingKeyOrOptions as PublishOptions);
    }

    this.channel.publish(
      this.options.exchangeName,
      routingKey,
      Buffer.from(JSON.stringify(message)),
      publishOptions,
    );
  }
}

export default PublisherClient;
