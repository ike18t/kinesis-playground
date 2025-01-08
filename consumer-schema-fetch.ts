import {
  GlueClient,
  GetSchemaVersionCommand,
  EntityNotFoundException,
} from "@aws-sdk/client-glue";
import { toTypeScript } from "@ovotech/avro-ts";
import { writeFileSync } from "fs";

import { config } from "./config";

(async () => {
  const client = new GlueClient({ region: config.awsRegion() });

  try {
    const response = await client.send(
      new GetSchemaVersionCommand({
        SchemaVersionNumber: {
          LatestVersion: true,
        },
        SchemaId: {
          SchemaName: config.schemaName(),
          RegistryName: config.registryName(),
        },
      })
    );

    if (response.SchemaDefinition === undefined) {
      throw new Error("Schema definition is undefined");
    }

    console.log(response.SchemaDefinition);

    writeFileSync(
      "consumer.gen.ts",
      toTypeScript(JSON.parse(response.SchemaDefinition))
    );
  } catch (error) {
    if (error instanceof EntityNotFoundException) {
      console.error(
        `Schema ${config.schemaName()} not found in ${config.registryName()} registry`
      );
    } else {
      console.error(error);
    }
  }
})();
