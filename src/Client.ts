import path from 'path';
import process from 'process';
import amqplib, { Channel, Connection, Options } from 'amqplib';

export interface ClientOptions {
  amqpUrl: string;
  exchangeName: string;
  exchangeType: string;
  exchangeOptions: Options.AssertExchange;
}

abstract class Client {
  protected readonly options: ClientOptions;

  protected connexion: Connection;

  protected channel: Channel;

  private static defaultOptions: ClientOptions = {
    amqpUrl: 'amqp://localhost',
    exchangeName: `${path.basename(process.cwd())}-exchange`,
    exchangeType: 'fanout',
    exchangeOptions: {
      durable: true,
    },
  };

  protected constructor(options: Partial<ClientOptions> = {}) {
    this.options = {
      ...Client.defaultOptions,
      ...options,
    };
  }

  async setup(): Promise<void> {
    this.connexion = await amqplib.connect(this.options.amqpUrl);
    this.channel = await this.connexion.createChannel();

    await this.channel.assertExchange(
      this.options.exchangeName,
      this.options.exchangeType,
      this.options.exchangeOptions || { durable: true },
    );
  }

  async tearDown(): Promise<void> {
    await this.channel.close();
    await this.connexion.close();
  }
}

export default Client;
