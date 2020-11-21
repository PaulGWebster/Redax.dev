using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        static ConcurrentDictionary<string,ulong> inboundPacketSequenceTracking =
            new ConcurrentDictionary<string,ulong> ();
        static Dictionary<string, ConcurrentDictionary<ulong, bool>> inboundPacketObserver =
            new Dictionary<string, ConcurrentDictionary<ulong, bool>>();
        static ConcurrentDictionary<int, double> websocketActivityTracker =
            new ConcurrentDictionary<int, double>();

        static void Main(string[] args)
        {
            // Populate inbound_queue_sequence_guard
            foreach (string currencyKey in common_objects.product_list()) {
                while (inboundPacketSequenceTracking.TryAdd(currencyKey, 0) == false)
                {
                    Thread.Sleep(1);
                }
                inboundPacketObserver.Add(currencyKey, new ConcurrentDictionary<ulong, bool>());
            }

            // Start a thread for getting information from the webscoket upstreams
            ThreadStart streamProcessorStart = 
                new ThreadStart(gdaxWebSocketFeed);
            Thread streamProcessor = 
                new Thread(streamProcessorStart);

            // Start a thread for tidying up the packet observer
            ThreadStart streamGarbageCollection =
                new ThreadStart(garbageMaintainance);
            Thread garbageProcessor =
                new Thread(streamGarbageCollection);

            // Wait for the threads
            garbageProcessor.Start();
            streamProcessor.Start();
            streamProcessor.Join();
        }

        private static void garbageMaintainance()
        {
            string[] currencyList = common_objects.product_list();

            while (true)
            {
                foreach (string currency in currencyList)
                {
                    if (inboundPacketObserver[currency].Count <= 100)
                    {
                        continue;
                    }
                    garbageMaintainance_cleanObserverDict(currency);
                }

                foreach (int websocketID in websocketActivityTracker.Keys)
                {
                    garbageMaintainance_checkWebsocketConnectState(websocketID);
                }
                    
                Thread.Sleep(500);
            }
        }

        private static void garbageMaintainance_checkWebsocketConnectState(int websocketID)
        {
            double currentEpoch = (DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;
            double delayInSeconds = currentEpoch - websocketActivityTracker[websocketID];

            Console.WriteLine("Websocket: {0}, delay: {1} seconds", websocketID, delayInSeconds) ;
        }

        private static void garbageMaintainance_cleanObserverDict(string currency)
        {
            foreach (ulong currencyKey in inboundPacketObserver[currency].Keys.OrderByDescending(x => x).Skip(100).ToList())
            {
                inboundPacketObserver[currency].TryRemove(currencyKey, out bool removalSuccess);
            }
        }

        private static void gdaxWebSocketFeed()
        {
            // Quick access to subscribe packet etc
            HelperClasses common_objects = new HelperClasses();

            // A place to store out websocket objects
            List<WebSocket> websocketStore =
                new List<WebSocket>();

            // Start three workers for websockets
            for (int i = 0;i <3;i++)
            {
                websocketActivityTracker.TryAdd(websocketStore.Count, 0);
                // Store the core object so it cannot go out of scope
                websocketStore.Add(NewConnection(websocketStore.Count));
            }

            // Do a check loop on our list for any new packets (this will be busy)
            while (true)
            {
                // Blockingly wait for a packet.
                InboundPacket inboundPacket = inbound_queue.Take();

                // Construct some information
                if (inboundPacket.type.Equals("open"))
                {
                    Subscribe subscribePacket = new Subscribe()
                    {
                        channels = new string[] { "full" },
                        product_ids = common_objects.product_list()
                    };
                    websocketStore[inboundPacket.id].Send(JsonConvert.SerializeObject(subscribePacket));
                }
                else if (inboundPacket.type.Equals("message"))
                {
                    GDAXPacket CastJSON = (GDAXPacket)inboundPacket.data;
                }
                Console.Title = string.Format("QueueSize: {0}", inbound_queue.Count);
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
                inbound_queue.Add(new InboundPacket(websocketID,"open"));
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
                // Mark the socket as active
                //websocketActivityTracker.TryAdd(websocketStore.Count, 0);
                websocketActivityTracker[websocketID] =
                    (DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalSeconds;

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
                    // string stringUniqueHash = product + sequence.ToString();

                    // Save that we seen the packet
                    inboundPacketObserver[product].AddOrUpdate(sequence, true, (key, oldValue) => true);

                    // Check if we care about this packet
                    if (
                        (inboundPacketSequenceTracking[product] == 0)
                        ||
                        (inboundPacketSequenceTracking[product]+1 == sequence)
                    )
                    {
                        // Instantly increment the guard check
                        inboundPacketSequenceTracking[product] = sequence;

                        // We need this packet, store it!
                        GDAXPacket CastJSON =
                            JsonConvert.DeserializeObject<GDAXPacket>(e.Data);

                        // Pop it into the queue
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
