import { PutRecordsCommand, KinesisClient } from "@aws-sdk/client-kinesis";
import {
  GlueSchemaRegistry,
  SchemaType,
  SchemaCompatibilityType,
} from "glue-schema-registry";
import { Type } from "avsc";

import { IkeTestNamespace } from "./schema.avsc.ts";
import { config } from "./config";

(async () => {
  const registry = new GlueSchemaRegistry<IkeTestNamespace.TestProperty>(
    config.registryName(),
    {
      region: config.awsRegion(),
    }
  );

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

  const kinesis = new KinesisClient({ region: config.awsRegion() });

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
            Data: await registry.encode(schemaId, message, { compress: true }),
            PartitionKey: "partition-key",
          },
        ],
      })
    );

    result.Records?.forEach((record) => {
      console.log(`Record message ${JSON.stringify(message, null, 2)}`);
      record.ShardId && console.log(`Record sent to shard ${record.ShardId}`);
      record.SequenceNumber &&
        console.log(`Record seq ${record.SequenceNumber}`);
    });
  };

  let i = 0;
  setInterval(async () => {
    await sendMessage({
      demo: `Message ${i++}`,
    });
  }, 1000);
})();
