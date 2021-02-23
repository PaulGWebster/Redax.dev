using System;
using System.Threading;
using System.Collections.Generic;

using GDAXWebsocketClient;
using Newtonsoft.Json;

namespace WStoTCPbridge
{
    class Program
    {
        //static redisClient rdclient;
        static List<gdaxWebsocket> wsclient;
        static int parallelConnections = 1;
        static Dictionary<string, UInt64> seqCheck = 
            new Dictionary<string, UInt64>();

        static string[] product_list = new string[] {
            "BTC-USD",
            "ETH-EUR",
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
        static string wsHost = "ws://nginx:80";
        static void Main(string[] args)
        {
            // Create a websocker reader
            wsclient = new List<gdaxWebsocket> ();

            // Start the workers
            for (int i = 0;i < parallelConnections;i++)
            {
                initWebsocketWorker(i);
            }

            while (true)
            {
                Thread.Sleep(1000);
            }
        }

        private static void initWebsocketWorker(int i)
        {
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

        private static void websocketMessage(string websocketEvent, UInt64 packet_seq, object[] message)
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
                string jsonAsString = (string)message[2];
                string currencyName = (string)message[1];

                // Check the sequence is initilized
                if (!seqCheck.ContainsKey(currencyName))
                {
                    Console.WriteLine("Initilized seqCheck for '{0}' starting at: {1}", currencyName, packet_seq);
                    seqCheck.Add(currencyName, packet_seq);
                }
                // Do not accept duplicate packets
                else if (packet_seq <= seqCheck[currencyName])
                {
                    return;
                }

                // Increment the rx count (should do this in handler)
                uint rxPacketCount = wsclient[websocketID].rxCount++;

                // every 15k records drop stats
                if (rxPacketCount % 15000 == 0)
                {
                    double initUT = 
                        ConvertToUnixTimestamp(wsclient[websocketID].initTime);
                    double currentUT =
                        ConvertToUnixTimestamp(DateTime.UtcNow);
                    double secondsPassed =
                        (currentUT - initUT);

                    Console.WriteLine(
                        "Socket({0}): count: {1} runtime: {2} seconds pps: {3}",
                        websocketID,
                        wsclient[websocketID].rxCount,
                        secondsPassed,
                        (wsclient[websocketID].rxCount/secondsPassed).ToString("0")
                    );
                }

                // Update the sequence
                seqCheck[currencyName] = packet_seq;
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

        private static DateTime ConvertFromUnixTimestamp(double timestamp)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            return origin.AddSeconds(timestamp);
        }

        private static double ConvertToUnixTimestamp(DateTime date)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            TimeSpan diff = date.ToUniversalTime() - origin;
            return Math.Floor(diff.TotalSeconds);
        }
    }
}