import moment from "moment";
import { config } from "dotenv";
import axios from "axios";
import { exists, getLatestHourTimestamp, redis } from "./utils";

config();

async function main() {
  const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY;
  const DUNE_API_KEY = process.env.DUNE_API_KEY;
  const MINT_QUERY_URL = process.env.MINT_QUERY_URL;

  let startDate = moment.utc("2023-06-01");
  let endDate = moment().endOf("day").add(1, "days");

  let mintResponse = (await redis.get("mints")) as {
    mint: string;
    earliest_transfer: any;
    swap_count: number;
  }[];

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

    const ohlcvStart = startOfMonthTimestamp + 3600;
    const ohlcvEnd = startOfNextMonthTimestamp;

    console.log(
      `Fetching: ${startDate.format("YYYY-MM")}`,
      ohlcvStart,
      ohlcvEnd,
    );

    for (let i = 1; i < mintResponse.length; i++) {
      let { mint, swap_count, earliest_transfer } = mintResponse[i];

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

      const items = data.data.items;

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
      for (let item of data.data.items) {
        if (!(item.v === 0 && item.unixTime > latestHourPassed)) {
          items.push(item);
        }
      }

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
}

main();
