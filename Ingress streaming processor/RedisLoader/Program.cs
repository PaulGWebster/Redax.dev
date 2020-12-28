using System;
using RedisLoader;
using GDAXWebsocketClient;
using Newtonsoft.Json;
using System.Threading;
using System.Collections.Generic;

namespace Redis_loader
{
    class Program
    {
        static redisClient rdclient;
        static List<gdaxWebsocket> wsclient;
        static ulong packetCount = 0;
        static DateTime startTimeDate = DateTime.UtcNow;
        static int parallelConnections = 4;
        static string[] product_list = new string[] {
            "BTC-USD",
            "ETH-EUR",
            "XRP-USD",
            "LTC-EUR",
            "BCH-USD",
            "EOS-EUR",
            "MKR-USD",
            "XLM-EUR",
            "XTZ-USD",
            "ETC-EUR",
            "OMG-USD",
            "LINK-EUR",
            "REP-USD",
            "ZRX-EUR",
            "ALGO-EUR",
            "DAI-USD",
            "COMP-USD",
            "BAND-EUR",
            "NMR-USD",
            "CGLD-EUR",
            "UMA-USD",
            "YFI-USD",
            "WBTC-USD",
            "ETH-BTC",
            "LTC-BTC",
            "EOS-BTC",
            "XLM-BTC",
            "XTZ-BTC",
            "OMG-BTC",
            "BAT-ETH",
            "ZRX-BTC",
            "COMP-BTC",
            "NMR-BTC",
            "UMA-BTC",
            "WBTC-BTC"
        };
        static string redisDSN = "redis:6379";
        static string wsHost = "ws://nginx:80";
        static void Main(string[] args)
        {
            // Connect to our storage database
            rdclient = new redisClient(redisDSN, redisMessage);
            if (!rdclient.IsConnected)
            {
                Console.WriteLine("Connection to redis failed.");
                Environment.Exit(1);
            }

            // Create a websocker reader
            wsclient = new List<gdaxWebsocket> ();
            initWebsocketWorker(0);

            while (true)
            {
                Thread.Sleep(1000);
            }
        }

        private static void initWebsocketWorker(int i)
        {
            if (i >= parallelConnections)
            {
                Console.WriteLine("Restricting websocket {0} from starting, > parallel count", i);
                return;
            }

            Console.WriteLine("[{0}] WebSocket connection initilizing",i);
            if (i < wsclient.Count)
            {
                wsclient[i] = new gdaxWebsocket(websocketMessage, i, wsHost, product_list);
            }
            else
            {
                wsclient.Add(new gdaxWebsocket(websocketMessage, i, wsHost, product_list));
            }
        }

        private static void redisMessage(string channel, string message)
        {
            //Console.WriteLine("REDIS({0}): {1}", channel, message);
        }

        private static void websocketMessage(string websocketEvent, ulong packet_seq, object[] message)
        {
            int websocketID = (int)message[0];
            if (websocketEvent.Equals("OPEN"))
            {
                Console.WriteLine("[{0}] Subscribing", websocketID);
                Subscribe subscribePacket = new Subscribe()
                {
                    channels = new string[] { "full" },
                    product_ids = product_list
                };
                string payload = JsonConvert.SerializeObject(subscribePacket);
                wsclient[websocketID].Send(payload);
            }
            else if (websocketEvent.Equals("MESSAGE"))
            {
                GDAXExchangePacket CastJSON = (GDAXExchangePacket)message[1];
                string jsonAsString = (string)message[2];
                rdclient.StringSetFireAndForget(
                    string.Join(":", CastJSON.product_id, CastJSON.sequence),
                    jsonAsString
                );

                // PubSub announce - All threads
                rdclient.Announce(jsonAsString);

                // Atomic increment hopefully
                packetCount++;

                // Management thread0 only
                if (websocketID == 0)
                {
                    if (
                        (packetCount * (ulong)wsclient.Count) % 15000 == 0 
                        && wsclient.Count < parallelConnections
                    )
                    {
                        initWebsocketWorker(wsclient.Count);
                    }
                    //else if ((packetCount % 2000) == 0)
                    //{
                    //    ulong runTimeInSeconds = (ulong)(DateTime.UtcNow - startTimeDate).TotalSeconds;
                    //    string commaNumberTotalPackets = string.Format("{0:n0}", packetCount);
                    //    string commaNumberPPS = string.Format("{0:n0}", packetCount / runTimeInSeconds);
                    //    Console.Title = string.Format("Packets processed: {0}, PPS: {1}", commaNumberTotalPackets, commaNumberPPS);
                    //}
                }
            }
            else if (websocketEvent.Equals("ERROR"))
            {
                string exceptionMsg = string.Empty;
                if (((string)message[1]).Equals("CLOSE"))
                {
                    exceptionMsg = (string)message[1];
                }
                else if (((string)message[1]).Equals("ERROR"))
                {
                    exceptionMsg = (string)message[1];
                }
                else
                {
                    exceptionMsg = "Unknown error";
                }
                
                Console.WriteLine("[{0}] Error detected: {1}",websocketID,exceptionMsg);
                initWebsocketWorker(websocketID);
            }
            else
            {
                Console.WriteLine("Unknown websocket event: {0}", websocketEvent);
            }
        }
    }
}