using System;
using RedisLoader;
using GDAXWebsocketClient;
using Newtonsoft.Json;

namespace Redis_loader
{
    class Program
    {
        static redisClient rdclient;
        static gdaxWebsocket[] wsclient;
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

        static void Main(string[] args)
        {
            string redisDSN = "10.200.200.1:6379";

            // Connect to our storage database
            rdclient = new redisClient(redisDSN, redisMessage);
            if (!rdclient.IsConnected)
            {
                Console.WriteLine("Connection to redis failed.");
                Environment.Exit(1);
            }

            // Create a websocker reader
            wsclient = new gdaxWebsocket[3];
            for (int i = 0;i < 3; i++)
            {
                wsclient[i] = new gdaxWebsocket(websocketMessage,i);
            }

            Console.ReadLine();
        }

        private static void redisMessage(string channel, string message)
        {
            Console.WriteLine("REDIS({0}): {1}", channel, message);
        }

        private static void websocketMessage(string websocketEvent, ulong packet_seq, object[] message)
        {
            int websocketID = (int)message[0];
            if (websocketEvent.Equals("OPEN"))
            {
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
                rdclient.StringSet(
                    string.Join(":",CastJSON.product_id,CastJSON.sequence),
                    jsonAsString
                );
                if ((packet_seq % 1000) == 0)
                {
                    Console.WriteLine("[{0}] {1} processed", websocketID,packet_seq);
                }
            }
            else if (websocketEvent.Equals("ERROR"))
            {
                Console.WriteLine("Error detected in websocket connection");
                Environment.Exit(1);
            }
            else if (websocketEvent.Equals("CLOSE"))
            {
                Console.WriteLine("Websocket closed connection");
                Environment.Exit(1);
            }
            else
            {
                Console.WriteLine("Unknown websocket event: {0}", websocketEvent);
            }
        }
    }
}