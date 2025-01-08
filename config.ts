require("dotenv-flow/config");
import { get } from "env-var";

export const config = {
  awsRegion: () => get("AWS_REGION").required().asString(),
  registryName: () => get("GLUE_REGISTRY_NAME").required().asString(),
  schemaName: () => get("GLUE_SCHEMA_NAME").required().asString(),
  streamName: () => get("STREAM_NAME").required().asString(),
};
