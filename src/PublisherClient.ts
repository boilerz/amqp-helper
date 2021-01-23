import { Options } from 'amqplib';

import Client, { ClientOptions } from './Client';

export interface PublishOptions extends Options.Publish {
  rootingKey?: string;
}

class PublisherClient<
  Msg extends Record<string, any>,
  RKey extends keyof any = ''
> extends Client {
  constructor(options: Partial<ClientOptions> = {}) {
    super(options);
  }

  static async createAndSetupClient<
    Msg extends Record<string, any>,
    RKey extends keyof any = ''
  >(options: Partial<ClientOptions> = {}): Promise<PublisherClient<Msg, RKey>> {
    const client = new PublisherClient<Msg, RKey>(options);
    await client.setup();

    return client;
  }

  async publish(message: Msg): Promise<void>;

  async publish(message: Msg, rootingKey: RKey): Promise<void>;

  async publish(message: Msg, options: PublishOptions): Promise<void>;

  async publish(
    message: Msg,
    rootingKeyOrOptions?: RKey | PublishOptions,
  ): Promise<void> {
    let publishOptions: Options.Publish = { persistent: true };
    let rootingKey = '';
    if (rootingKeyOrOptions) {
      const rootingKeyProvided = typeof rootingKeyOrOptions === 'string';
      rootingKey = rootingKeyProvided
        ? (rootingKeyOrOptions as string)
        : (rootingKeyOrOptions as PublishOptions).rootingKey || '';
      publishOptions = rootingKeyProvided
        ? { persistent: true }
        : (rootingKeyOrOptions as PublishOptions);
    }

    this.channel.publish(
      this.options.exchangeName,
      rootingKey,
      Buffer.from(JSON.stringify(message)),
      publishOptions,
    );
  }
}

export default PublisherClient;
