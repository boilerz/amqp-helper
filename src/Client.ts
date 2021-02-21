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

  protected _connection?: Connection;

  protected _channel?: Channel;

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

  get channel(): Channel {
    if (!this._channel) throw new Error('Channel not initialized');
    return this._channel;
  }

  get connection(): Connection {
    if (!this._connection) throw new Error('Connexion not initialized');
    return this._connection;
  }

  async setup(): Promise<void> {
    this._connection = await amqplib.connect(this.options.amqpUrl);
    this._channel = await this.connection.createChannel();

    await this.channel.assertExchange(
      this.options.exchangeName,
      this.options.exchangeType,
      this.options.exchangeOptions || { durable: true },
    );
  }

  async tearDown(): Promise<void> {
    await this.channel.close();
    await this.connection.close();
  }
}

export default Client;
