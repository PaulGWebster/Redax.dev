using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using Newtonsoft.Json;
using Websocket_Record_Standard;
using WebSocketSharp;

namespace Websocket_Record
{
    class Program
    {
        static HelperClasses common_objects =
            new HelperClasses();

        static BlockingCollection<InboundPacket> inbound_queue =
            new BlockingCollection<InboundPacket>();
        static ConcurrentDictionary<string,ulong> inbound_queue_sequence_guard =
            new ConcurrentDictionary<string,ulong> ();

        static void Main(string[] args)
        {
            // Populate inbound_queue_sequence_guard
            foreach (string currencyKey in common_objects.product_list()) {
                while (inbound_queue_sequence_guard.TryAdd(currencyKey, 0) == false)
                {
                    Thread.Sleep(1);
                }
            }

            // Start a thread for getting information from the webscoket upstreams
            ThreadStart streamProcessorStart = 
                new ThreadStart(gdaxWebSocketFeed);
            Thread streamProcessor = 
                new Thread(streamProcessorStart);

            // Wait for the threads
            streamProcessor.Start();
            streamProcessor.Join();
        }
        private static void gdaxWebSocketFeed()
        {
            // Quick access to subscribe packet etc
            HelperClasses common_objects = new HelperClasses();

            // A place to store out websocket objects
            List<WebSocket> websocketStore =
                new List<WebSocket>();

            // Start three workers for websockets
            for (int i = 2;i <3;i++)
            {
                // Store the core object so it cannot go out of scope
                websocketStore.Add(NewConnection(websocketStore.Count));
            }

            // Do a check loop on our list for any new packets (this will be busy)
            while (true)
            {
                InboundPacket jsonpacket = inbound_queue.Take();
                // You can only get 
                int websocketId = jsonpacket.id;

            }
        }
        private static WebSocket NewConnection(int websocketID)
        {
            WebSocket websocketObj =
                new WebSocket("wss://ws-feed.pro.coinbase.com");
            websocketObj.SslConfiguration.EnabledSslProtocols =
                System.Security.Authentication.SslProtocols.Tls12;
            websocketObj.EmitOnPing =
                false;

            websocketObj.OnOpen += (sender, e) =>
            {
                // Here should query what packets is availible and subscribe to them all!
                Subscribe subscribePacket = new Subscribe()
                {
                    channels = new string[] { "full" },
                    product_ids = common_objects.product_list()
                };
                inbound_queue.Add(new InboundPacket(websocketID,"open"));
                websocketObj.Send(JsonConvert.SerializeObject(subscribePacket));
            };
            websocketObj.OnError += (sender, e) =>
            {
                inbound_queue.Add(new InboundPacket(websocketID,"error"));
            };
            websocketObj.OnClose += (sender, e) =>
            {
                inbound_queue.Add(new InboundPacket(websocketID,"close"));
            };
            websocketObj.OnMessage += (sender, e) =>
            {
                // Create a nicer name for e.data
                string rawJson = e.Data;

                // Extract the sequence and product_id from the raw json
                string type = extractValueFromJson(rawJson, "type");

                // If we are of type 'subscriptions' then ignore
                if (type.Equals("subscriptions"))
                {
                    Console.WriteLine("Subscriptions found!");
                }
                else
                {
                    // Create a hash that is the currency and sequence_id
                    ulong sequence = Convert.ToUInt64(extractValueFromJson(rawJson, "sequence"));
                    string product = extractValueFromJson(rawJson, "product_id");

                    // Create the unique hashkey
                    string stringUniqueHash = product + sequence.ToString();

                    // Check if we care about this packet
                    if (inbound_queue_sequence_guard[product] == sequence+1)
                    {
                        // Instantly increment the guard check
                        inbound_queue_sequence_guard[product]++;

                        // We need this packet, store it!
                        GDAXPacket CastJSON =
                            JsonConvert.DeserializeObject<GDAXPacket>(e.Data);

                        inbound_queue.Add(new InboundPacket(websocketID, "message", CastJSON));
                    }
                }
            };
            websocketObj.Connect();
            return websocketObj;
        }
        private static string extractValueFromJson(string input,string key)
        {
            int initialIndex = input.IndexOf(key) + key.Length + 1;
            int finalIndex = input.IndexOf(",", initialIndex);

            if (input.Substring(initialIndex,2).Equals(":\""))
            {
                initialIndex += 1;
                finalIndex -= 1;
            }

            initialIndex += 1;
            finalIndex -= initialIndex;

            return input.Substring(initialIndex, finalIndex);
        }
    }


}
