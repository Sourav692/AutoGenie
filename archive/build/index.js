import { analytics, createApp, server } from "@databricks/appkit";

//#region server/index.ts
await createApp({ plugins: [server(), analytics()] });

//#endregion
export {  };