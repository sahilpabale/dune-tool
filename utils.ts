import { config } from "dotenv";
import { Redis } from "@upstash/redis";
import moment from "moment";

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
