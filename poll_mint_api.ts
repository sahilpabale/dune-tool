// this poller is a cron service that helps in polling the mint api to get the list of mints and check if there's a change in the list and update the mint store accordingly

import { config } from "dotenv";
import axios from "axios";
import { CronJob } from "cron";
import { redis } from "./redis";

config();

const poll = async () => {
  try {
    const DUNE_API_KEY = process.env.DUNE_API_KEY;
    const MINT_QUERY_URL = process.env.MINT_QUERY_URL;

    let mintResponse = await axios.get(MINT_QUERY_URL as string, {
      headers: {
        "X-Dune-API-Key": DUNE_API_KEY,
      },
    });

    mintResponse = mintResponse.data.result.rows.filter(
      (row: any) => row.swap_count > 200,
    );

    const storedMints = await redis.get("mints");

    if (storedMints === null) {
      console.log("No mints stored yet");
      await redis.set("mints", JSON.stringify(mintResponse));
      return;
    }

    if (JSON.stringify(storedMints) === JSON.stringify(mintResponse)) {
      console.log("No change in mints");
      return;
    }

    console.log("Mints have changed...updating store");

    await redis.set("mints", JSON.stringify(mintResponse));
  } catch (error) {
    console.error("failed to poll the new mints: ", error);
  }
};

const job = new CronJob(
  "*/1 * * * *", // cronTime
  poll, // onTick
  null, // onComplete
  true, // start
  "Europe/London", // timeZone
);

job.start();
