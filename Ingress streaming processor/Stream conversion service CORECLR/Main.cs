using System;
using System.Threading;

using System.Configuration;
using WebsocketManager;
using ConcurrentMessageBus;
using GDAX;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Stream_conversion_service_CORECLR
{
    class Program
    {
        // Static objects
        static Configuration configManager =
            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
        static KeyValueConfigurationCollection confCollection =
            configManager.AppSettings.Settings;

        static void Main(string[] args)
        {
            // Create a dictionary for our websocket workers
            Dictionary<string, workerContainer> workers = new Dictionary<string, workerContainer>();
            // We need to stash at least 3 of the stach somewhere so we can have triple ingress into streamProcessorFunction.

            // Start a thread for handling data from the websocket clients
            ThreadStart streamProcessorStart = new ThreadStart(streamProcessorFunction);
            Thread streamProcessor = new Thread(streamProcessorStart);

            // Wait for the threads
            streamProcessor.Start();
            streamProcessor.Join();
        }

        private static void streamProcessorFunction()
        {
            // Start websocket connectors - not saving packets, pass reference back to a function on main
            PairedBiDirectionalBus masterQueue = new PairedBiDirectionalBus();
            int senderQueue = masterQueue.Register();

            websocketInterface wsCtrl = new websocketInterface(masterQueue);
            //wsCtrl.New(URI: "wss://ws-feed.pro.coinbase.com", Tag: "GDAX1", Queue: masterQueue);
            //wsCtrl.Start("GDAX1");

            //// Subscription channels
            //string[] product_list = new string[] {
            //    "BTC-USD",
            //    "ETH-EUR",
            //    "XRP-USD",
            //    "LTC-EUR",
            //    "BCH-USD",
            //    "EOS-EUR",
            //    "MKR-USD",
            //    "XLM-EUR",
            //    "XTZ-USD",
            //    "ETC-EUR",
            //    "OMG-USD",
            //    "LINK-EUR",
            //    "REP-USD",
            //    "ZRX-EUR",
            //    "ALGO-EUR",
            //    "DAI-USD",
            //    "COMP-USD",
            //    "BAND-EUR",
            //    "NMR-USD",
            //    "CGLD-EUR",
            //    "UMA-USD",
            //    "YFI-USD",
            //    "WBTC-USD",
            //    "ETH-BTC",
            //    "LTC-BTC",
            //    "EOS-BTC",
            //    "XLM-BTC",
            //    "XTZ-BTC",
            //    "OMG-BTC",
            //    "BAT-ETH",
            //    "ZRX-BTC",
            //    "COMP-BTC",
            //    "NMR-BTC",
            //    "UMA-BTC",
            //    "WBTC-BTC"
            //};

            //// Heavest packets from the sockets
            //while (true)
            //{
            //    BiDireactionalBusPacket packet = masterQueue.Pull(senderQueue);
            //    if (packet == null) { continue; }
            //    if (packet.Type == 10)
            //    {
            //        // We are connected! lets send a connection request
            //        Subscribe subscribePacket = new Subscribe()
            //        {
            //            channels = new string[] { "full" },
            //            product_ids = product_list
            //        };
            //        // Encode and send
            //        masterQueue.Push(masterQueue.pairPartner(senderQueue), 20, JsonConvert.SerializeObject(subscribePacket), null);
            //    }
            //    else if (packet.Type == 13)
            //    {
            //        // On message 13 we recieved a packet for the websocket producer that is an active trade action
            //        // We must try to file this into a serialised bus for sending to consumers.

            //    }
            //    else
            //    {
            //        Console.WriteLine("Got[{0}]: {1}", packet.Type, (string)packet.Data);
            Thread.Sleep(1000 * 180);
        }
    }

    internal class workerContainer
    {
    }
}
