import moment from "moment";
import { config } from "dotenv";
import axios from "axios";
import {
  convertUnixTimestampToUTC,
  exists,
  getLatestHourTimestamp,
  jsonToCsv,
  redis,
} from "./utils";
import { DuneClient, ContentType } from "@duneanalytics/client-sdk";

config();

async function main() {
  const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY;
  const DUNE_API_KEY = process.env.DUNE_API_KEY;

  const dune = new DuneClient(DUNE_API_KEY!);

  let startDate = moment.utc("2023-06-01");
  let endDate = moment().endOf("day").add(1, "days");

  let mintResponse = (await redis.get("mints")) as {
    mint: string;
    earliest_transfer: any;
    swap_count: number;
  }[];

  // Load or initialize checkpoints from Redis
  let checkpoints: { [mint: string]: number } = {};
  const savedCheckpoints = await redis.get("checkpoints");
  if (savedCheckpoints) {
    console.log(savedCheckpoints);
    checkpoints = savedCheckpoints as { [mint: string]: number };
  }

  while (startDate.isBefore(endDate) || startDate.isSame(endDate, "month")) {
    // Unix timestamp for the beginning of the month
    const startOfMonthTimestamp = startDate.startOf("month").unix();

    // Unix timestamp for the end of the month. It's important to clone the moment
    // because startOf and endOf modify the moment instance itself.
    const startOfNextMonthTimestamp = startDate
      .clone()
      .add(1, "months")
      .startOf("month")
      .unix();

    // const ohlcvStart = startOfMonthTimestamp + 3600;
    // const ohlcvEnd = startOfNextMonthTimestamp;

    // console.log(
    //   `Fetching: ${startDate.format("YYYY-MM")}`,
    //   ohlcvStart,
    //   ohlcvEnd,
    // );

    for (let i = 1; i < mintResponse.length; i++) {
      let { mint, swap_count, earliest_transfer } = mintResponse[i];

      const lastFetchedTimestamp = checkpoints[mint] || startOfMonthTimestamp; // Get last fetched timestamp or start of month as default

      const ohlcvStart = lastFetchedTimestamp + 3600; // Start from the last fetched timestamp
      const ohlcvEnd = startOfNextMonthTimestamp;

      console.log(`Fetching: ${startDate.format("YYYY-MM")} for mint ${mint}`);

      earliest_transfer = moment.utc(
        earliest_transfer,
        "YYYY-MM-DD HH:mm:ss.SSS [UTC]",
      );

      if (swap_count < 200) {
        continue;
      }

      if (earliest_transfer.unix() > ohlcvEnd) {
        console.log(
          "Skiping because of earliest transfer",
          mint,
          swap_count,
          startDate.format("YYYY-MM-DD"),
          ohlcvStart,
          ohlcvEnd,
        );

        continue;
      }

      // fetch only if file not exists already
      if (await exists(`${mint}_${ohlcvStart}_${ohlcvEnd}`)) {
        console.log(
          "Already Exists",
          mint,
          swap_count,
          startDate.format("YYYY-MM-DD"),
          ohlcvStart,
          ohlcvEnd,
        );
        continue;
      }

      const config = {
        headers: {
          "x-api-key": BIRDEYE_API_KEY,
        },
        params: {
          address: mint,
          type: "1H",
          time_from: ohlcvStart,
          time_to: ohlcvEnd,
        },
      };

      console.log("Fetching", mint, ohlcvStart, ohlcvEnd);

      const { data } = await axios.get(
        "https://public-api.birdeye.so/defi/ohlcv",
        config,
      );

      let items: any[] = [];

      // Update checkpoint to the last data point timestamp
      if (data.data.items.length > 0) {
        const lastDataPointTimestamp =
          data.data.items[data.data.items.length - 1].unixTime;
        checkpoints[mint] = lastDataPointTimestamp;

        // Save updated checkpoints to Redis after each successful fetch
        await redis.set("checkpoints", JSON.stringify(checkpoints));
      }

      if (data.data.items.length === 0) {
        console.log(
          "No Data",
          mint,
          swap_count,
          startDate.format("YYYY-MM-DD"),
          ohlcvStart,
          ohlcvEnd,
        );
        continue;
      }

      let latestHourPassed = getLatestHourTimestamp(); // get the latest passed hour timestamp

      // check for duplicates
      items = data.data.items.filter(
        (item: any) => !(item.v === 0 && item.unixTime > latestHourPassed),
      );

      const lastDataPoint = items[items.length - 1].unixTime; // new items array

      if (lastDataPoint === ohlcvEnd) {
        if (await exists(`${mint}_${ohlcvStart}`)) {
          // we are removing an uncompleted period with a completed one
          await redis.unlink(`${mint}_${ohlcvStart}`);
        }

        console.log(
          "Persisting Complete Period",
          mint,
          swap_count,
          startDate.format("YYYY-MM-DD"),
          ohlcvStart,
          ohlcvEnd,
        );
        await redis.set(`${mint}_${ohlcvStart}_${ohlcvEnd}`, data);
        let csvData = data.data.items.map((item: any) => {
          return {
            address: item.address,
            unixTime: convertUnixTimestampToUTC(item.unixTime),
            o: item.o,
            h: item.h,
            l: item.l,
            c: item.c,
            v: item.v,
          };
        });
        // upload the csv to dune (data.data.items)
        let csv = jsonToCsv(csvData);

        const inserted = await dune.table.insert({
          table_name: "prices",
          namespace: "sahilpabale",
          data: Buffer.from(csv),
          content_type: ContentType.Csv,
        });

        console.log(`uploaded to dune  -`, inserted.rows_written);
      } else {
        // persist an incomplete period
        console.log(
          "Persisting Incomplete Period",
          mint,
          swap_count,
          startDate.format("YYYY-MM-DD"),
          ohlcvStart,
          ohlcvEnd,
        );
        await redis.set(`${mint}_${ohlcvStart}`, data);
      }
    }

    // Move to the next month
    startDate.add(1, "months");
  }

  // Save updated checkpoints to Redis
  await redis.set("checkpoints", JSON.stringify(checkpoints));
}

main();
