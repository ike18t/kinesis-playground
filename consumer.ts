import {
  GetRecordsCommand,
  GetShardIteratorCommand,
  KinesisClient,
  ListShardsCommand,
} from "@aws-sdk/client-kinesis";
import { GlueSchemaRegistry } from "glue-schema-registry";
import { Type } from "avsc";

import { config } from "./config";
import { IkeTestNamespace } from "./consumer.gen";

const registry = new GlueSchemaRegistry<IkeTestNamespace.TestProperty>(
  config.registryName(),
  {
    region: config.awsRegion(),
  }
);
const schema = Type.forSchema(JSON.parse(IkeTestNamespace.TestPropertySchema));

const kinesis = new KinesisClient({ region: config.awsRegion() });

(async () => {
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

  let lastSequenceNumber: string | undefined;
  const { Shards: shards } = await kinesis.send(
    new ListShardsCommand({ StreamName: config.streamName() })
  );

  shards?.map(async ({ ShardId: shardId }) => {
    while (true && shardId) {
      lastSequenceNumber = await receiveMessages(
        config.streamName(),
        shardId,
        lastSequenceNumber
      );
    }
  });
})();
