﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json;
using StackExchange.Redis;
using WebSocketSharp;

namespace RedisLoader
{
    public class redisClient
    {
        private string redisDSN;
        private ConnectionMultiplexer redisObj;
        private IDatabase redisDB;
        private ISubscriber pubsubInterface;

        private Guid redisID = Guid.NewGuid();

        public redisClient(string redisDSN, Action<string,string> announceFunction)
        {
            this.redisDSN = redisDSN;
            redisObj = ConnectionMultiplexer.Connect(redisDSN);
            redisDB = redisObj.GetDatabase(0);
            pubsubInterface = redisObj.GetSubscriber();

            pubsubInterface.Subscribe(
                "streamraid",
                (channel, message) => {
                    announceFunction(channel.ToString(), message.ToString());
                }
            );

            pubsubInterface.Publish("streamraid", "NEWSRC "+redisID.ToString());
        }
        public Guid id
        {
            get
            {
                return redisID;
            }
        }
        public bool IsConnected
        {
            get
            {
                return redisObj.IsConnected;
            }
        }

        public void StringSetFireAndForget(
            string      key,
            string      value,
            int hours   = 1,
            int minutes = 0,
            int seconds = 0
        )
        {
            redisDB.StringSet(
                key,
                value,
                new TimeSpan(hours, minutes, seconds),
                flags: CommandFlags.FireAndForget
            );
        }
    }
}

namespace GDAXWebsocketClient
{
    public class gdaxWebsocket
    {
        public static Action<string, ulong, object[]> parentFunction;
        private static Dictionary<int,ConcurrentQueue<string>> SendQueue = 
            new Dictionary<int,ConcurrentQueue<string>> ();

        public int websocketID { get; }

        public gdaxWebsocket(Action<string, ulong, object[]> announceFunction, int wsID) { 
            parentFunction = announceFunction;

            ParameterizedThreadStart streamProcessorStart =
                new ParameterizedThreadStart(gdaxWebSocketFeed);
            Thread streamProcessor =
                new Thread(streamProcessorStart);

            websocketID = wsID;
            SendQueue.Add(websocketID, new ConcurrentQueue<string> ());

            streamProcessor.Start(new object[] { websocketID, SendQueue });
        }

        private void gdaxWebSocketFeed(object passedArgs)
        {
            object[] passedArgsArray = (object[])passedArgs;

            ulong packetID = 0;
            int websocketID = (int)passedArgsArray[0];
            Dictionary<int,ConcurrentQueue<string>> sendQueue = 
                (Dictionary<int,ConcurrentQueue<string>>)passedArgsArray[1];

            WebSocket wsClient = new WebSocket("wss://ws-feed.pro.coinbase.com");
            wsClient.SslConfiguration.EnabledSslProtocols =
                System.Security.Authentication.SslProtocols.Tls12;

            wsClient.OnOpen += (sender, e) =>
            {
                parentFunction("OPEN", packetID++, new object[] { websocketID, e });
            };
            wsClient.OnError += (sender, e) =>
            {
                parentFunction("ERROR", packetID++, new object[] { websocketID, e });
            };
            wsClient.OnClose += (sender, e) =>
            {
                parentFunction("CLOSE", packetID++, new object[] { websocketID, e });
            };
            wsClient.OnMessage += (sender, e) =>
            {
                GDAXExchangePacket CastJSON =
                    JsonConvert.DeserializeObject<GDAXExchangePacket>(e.Data);
                parentFunction("MESSAGE", packetID++, new object[] { websocketID, CastJSON, e.Data });
            };

            wsClient.Connect();

            while (wsClient.IsAlive)
            {
                if (sendQueue.Count == 0)
                {
                    Thread.Sleep(100);
                }
                else if (sendQueue[websocketID].TryDequeue(out string sendData)) { 
                    if (sendData != null)
                    {
                        wsClient.Send(sendData);
                    }
                }
            }
        }

        public void Send(string payload)
        {
            SendQueue[websocketID].Enqueue(payload);
        }
    }
    public class GDAXExchangePacket
    {
        #pragma warning disable CS0649
        public string type;
        public string side;
        public string product_id;
        public DateTime time;
        public SubscriptionChannel[] channels;
        public long sequence;
        public string order_id;
        public string order_type;
        public string new_size;
        public string old_size;
        public string timestamp;
        public string stop_type;
        public string stop_price;
        public string size;
        public string price;
        public string cliend_oid;
        public string trade_id;
        public string maker_order_id;
        public string taker_order_id;
        public string funds;
        public string taker_fee_rate;
        public string @private;
        public string remaining_size;
        public string reason;
        public string user_id;
        #pragma warning restore CS0649
    }
    public class Subscribe
    {
        public readonly string type = "subscribe";
        public string[] product_ids { get; set; }
        public string[] channels { get; set; }
    }
    public class SubscriptionChannel
    {
        public string name { get; set; }
        public string[] product_ids { get; set; }
    }
}