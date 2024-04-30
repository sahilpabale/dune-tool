import { config } from "dotenv";
import { Redis } from "@upstash/redis";

config();

export const redis = new Redis({
  url: process.env.UPSTASH_URL as string,
  token: process.env.UPSTASH_TOKEN as string,
});
