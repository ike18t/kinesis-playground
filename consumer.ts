import {
  GetRecordsCommand,
  GetShardIteratorCommand,
  KinesisClient,
  ListShardsCommand,
} from "@aws-sdk/client-kinesis";
import { GlueSchemaRegistry } from "@meinestadt.de/glue-schema-registry";
import { Type } from "avsc";

import { config } from "./config";
import { IkeTestNamespace } from "./consumer.gen";

class LimitedMap<K, V> extends Map {
  constructor(private limit: number) {
    super();
  }

  set(key: K, value: V): this {
    if (this.atLimit()) {
      throw new Error("Map size limit exceeded");
    }
    super.set(key, value);
    return this;
  }

  atLimit() {
    return this.size >= this.limit;
  }
}

const registry = new GlueSchemaRegistry<IkeTestNamespace.TestProperty>(
  config.registryName(),
  {
    region: config.awsRegion(),
  }
);
const schema = Type.forSchema(JSON.parse(IkeTestNamespace.TestPropertySchema));

const kinesis = new KinesisClient({ region: config.awsRegion() });

const receiveMessages = async (
  streamName: string,
  shardId: string,
  startingSequenceNumber?: string
): Promise<string | undefined> => {
  const shardIterator = (
    await kinesis.send(
      new GetShardIteratorCommand({
        StreamName: streamName,
        ShardId: shardId,
        ...(startingSequenceNumber
          ? {
              ShardIteratorType: "AFTER_SEQUENCE_NUMBER",
              StartingSequenceNumber: startingSequenceNumber,
            }
          : { ShardIteratorType: "LATEST" }),
      })
    )
  )?.ShardIterator;

  if (shardIterator) {
    const records =
      (
        await kinesis.send(
          new GetRecordsCommand({
            ShardIterator: shardIterator,
            Limit: 10,
          })
        )
      )?.Records || [];

    let lastSequenceNumber: string | undefined;

    for (const record of records) {
      if (!record.Data || !record.SequenceNumber) {
        continue;
      }

      lastSequenceNumber = record.SequenceNumber;

      const message = await registry.decode(Buffer.from(record.Data), schema);
      console.log({ message, shardId, lastSequenceNumber });
    }

    return lastSequenceNumber ?? startingSequenceNumber;
  }

  return startingSequenceNumber;
};

const poolSize = 2;
const leaseMap = new LimitedMap<string, Promise<unknown>>(poolSize);
const sequenceKeeper = new Map<string, string>();

setInterval(async () => {
  if (leaseMap.atLimit()) {
    return;
  }

  const { Shards: shards } = await kinesis.send(
    new ListShardsCommand({ StreamName: config.streamName() })
  );

  shards?.forEach(({ ShardId: shardId }) => {
    if (leaseMap.atLimit()) {
      return;
    }

    if (shardId && !leaseMap.has(shardId)) {
      const promise = async () => {
        while (true) {
          try {
            const sequenceNumber = await receiveMessages(
              config.streamName(),
              shardId,
              sequenceKeeper.get(shardId)
            );

            if (sequenceNumber) {
              sequenceKeeper.set(shardId, sequenceNumber);
            }
          } catch (error) {
            leaseMap.delete(shardId);
            throw error;
          }
        }
      };

      leaseMap.set(shardId, promise());
    }
  });
});
