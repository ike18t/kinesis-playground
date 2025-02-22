import { PutRecordsCommand, KinesisClient } from "@aws-sdk/client-kinesis";
import {
  GlueSchemaRegistry,
  SchemaType,
  SchemaCompatibilityType,
} from "@meinestadt.de/glue-schema-registry";
import { Type } from "avsc";

import { IkeTestNamespace } from "./schema.avsc.ts";
import { config } from "./config";

const registry = new GlueSchemaRegistry<IkeTestNamespace.TestProperty>(
  config.registryName(),
  {
    region: config.awsRegion(),
  }
);

const kinesis = new KinesisClient({ region: config.awsRegion() });

(async () => {
  let schemaId: string | undefined;

  try {
    schemaId = await registry.createSchema({
      type: SchemaType.AVRO,
      schemaName: config.schemaName(),
      compatibility: SchemaCompatibilityType.FULL,
      schema: IkeTestNamespace.TestPropertySchema,
    });
  } catch (error) {
    schemaId = await registry.register({
      schemaName: config.schemaName(),
      type: SchemaType.AVRO,
      schema: IkeTestNamespace.TestPropertySchema,
    });
  }

  if (schemaId === undefined) {
    throw new Error("Schema ID is undefined");
  }

  const sendMessage = async (message: IkeTestNamespace.TestProperty) => {
    const type = Type.forSchema(
      JSON.parse(IkeTestNamespace.TestPropertySchema)
    );

    if (!type.isValid(message)) {
      throw new Error("Invalid message");
    }

    const result = await kinesis.send(
      new PutRecordsCommand({
        StreamName: config.streamName(),
        Records: [
          {
            Data: await registry.encode(schemaId, message),
            PartitionKey: `partition-key-${[Math.floor(Math.random() * 10)]}`,
          },
        ],
      })
    );

    result.Records?.forEach((record) => {
      console.log(`\nRecord sent`);
      console.log(`message ${JSON.stringify(message, null, 2)}`);
      record.ShardId && console.log(`shard ${record.ShardId}`);
      record.SequenceNumber && console.log(`seq ${record.SequenceNumber}`);
    });
  };

  let i = 0;
  setInterval(async () => {
    await sendMessage({
      demo: `Message ${i++}`,
    });
  }, 1000);
})();
