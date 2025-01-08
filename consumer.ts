import {
  GetShardIteratorCommand,
  GetRecordsCommand,
  KinesisClient,
} from "@aws-sdk/client-kinesis";
import { GlueSchemaRegistry } from "glue-schema-registry";
import { Type } from "avsc";

import { config } from "./config";
import { IkeTestNamespace } from "./consumer.gen";

(async () => {
  const registry = new GlueSchemaRegistry<IkeTestNamespace.TestProperty>(
    config.registryName(),
    {
      region: config.awsRegion(),
    }
  );
  const schema = Type.forSchema(
    JSON.parse(IkeTestNamespace.TestPropertySchema)
  );

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
                ShardIteratorType: "AT_SEQUENCE_NUMBER",
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
        lastSequenceNumber = record.SequenceNumber;

        if (!record.Data) {
          continue;
        }

        const message = await registry.decode(Buffer.from(record.Data), schema);
        console.log({ message, lastSequenceNumber });
      }

      return lastSequenceNumber;
    }

    return;
  };

  let lastSequenceNumber: string | undefined;
  while (true) {
    lastSequenceNumber = await receiveMessages(
      config.streamName(),
      "shardId-000000000000",
      lastSequenceNumber
    );
  }
})();
