import { config } from "dotenv";
import { Redis } from "@upstash/redis";
import moment from "moment";
import { DuneClient, ColumnType } from "@duneanalytics/client-sdk";

config();

export const redis = new Redis({
  url: process.env.UPSTASH_URL as string,
  token: process.env.UPSTASH_TOKEN as string,
});

export const exists = async (key: string) => {
  return (await redis.exists(key)) === 1 ? true : false;
};

export const formatNumber = (input: number) => {
  return input.toFixed(15);
};

export const convertUnixTimestampToUTC = (unixTimestamp: number) => {
  // Create a new Date object from the Unix timestamp (in milliseconds)
  const date = new Date(unixTimestamp * 1000);

  // Format the date and time parts to match the desired format
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0"); // Months are 0-indexed, add 1
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");
  const milliseconds = String(date.getUTCMilliseconds()).padStart(3, "0");

  // Combine parts into the final string
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds} UTC`;
};

export const getLatestHourTimestamp = (): number => {
  const currentMoment = moment();
  const currentHour = currentMoment.hour();
  const latestHourMoment = currentMoment.startOf("day").hour(currentHour);

  // If the latest hour is in the future, subtract one hour
  if (latestHourMoment.isAfter(currentMoment)) {
    latestHourMoment.subtract(1, "hour");
  }

  return latestHourMoment.unix();
};

export const jsonToCsv = (items: any) => {
  const header = "address,unixTime,o,h,l,c,v".split(",");
  const headerString = "mint,block_time,o,h,l,c,v";
  // handle null or undefined values here
  const replacer = (key: any, value: any) => value ?? "";
  const rowItems = items.map((row: any) =>
    header
      .map((fieldName) => JSON.stringify(row[fieldName], replacer))
      .join(","),
  );
  // join header and body, and break into separate lines
  return [headerString, ...rowItems].join("\r\n");
};

export const findDifference = <T extends Record<string, any>>(
  array1: T[],
  array2: T[],
) => {
  const difference: T[] = [];

  for (const obj2 of array2) {
    let found = false;
    for (const obj1 of array1) {
      if (obj1.mint === obj2.mint && obj1.time === obj2.time) {
        found = true;
        break;
      }
    }
    if (!found) {
      difference.push(obj2);
    }
  }

  return difference;
};

export const createTable = async (client: DuneClient, table_name: string) => {
  const result = await client.table.create({
    namespace: "sahilpabale",
    table_name,
    schema: [
      { name: "mint", type: ColumnType.Varchar },
      { name: "block_time", type: ColumnType.Varchar },
      { name: "o", type: ColumnType.Double },
      { name: "h", type: ColumnType.Double },
      { name: "l", type: ColumnType.Double },
      { name: "c", type: ColumnType.Double },
      { name: "v", type: ColumnType.Double },
    ],
  });

  return result;
};
