using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using WebSocketSharp;

namespace GDAXWebsocketClient
{
    public class gdaxWebsocket
    {
        public static Action<string, ulong, object[]> parentFunction;
        private static Dictionary<int,ConcurrentQueue<string>> SendQueue = 
            new Dictionary<int,ConcurrentQueue<string>> ();
        public uint rxCount { get; set; }
        public DateTime initTime = DateTime.UtcNow;

        public int websocketID { get; }

        public gdaxWebsocket(
            Action<string, ulong, object[]> announceFunction,
            int wsID,
            string wsHost,
            string[] product_list
        )   { 
            parentFunction = announceFunction;

            ParameterizedThreadStart streamProcessorStart =
                new ParameterizedThreadStart(gdaxWebSocketFeed);
            Thread streamProcessor =
                new Thread(streamProcessorStart);

            websocketID = wsID;
            if (SendQueue.ContainsKey(websocketID))
            {
                SendQueue[websocketID] = new ConcurrentQueue<string>();
            }
            else
            {
                SendQueue.Add(websocketID, new ConcurrentQueue<string>());
            }

            streamProcessor.Start(new object[] { websocketID, SendQueue, wsHost });
        }

        private void gdaxWebSocketFeed(object passedArgs)
        {
            object[] passedArgsArray = (object[])passedArgs;

            ulong packetID = 0;
            int websocketID = (int)passedArgsArray[0];
            Dictionary<int,ConcurrentQueue<string>> sendQueue = 
                (Dictionary<int,ConcurrentQueue<string>>)passedArgsArray[1];
            string wsHost = (string)passedArgsArray[2];
            bool initialPacket = true;

            WebSocket wsClient = new WebSocket(wsHost);

            wsClient.OnOpen += (sender, e) =>
            {
                parentFunction("OPEN", packetID++, new object[] { websocketID, e });
            };
            wsClient.OnError += (sender, e) =>
            {
                parentFunction("ERROR", packetID++, new object[] { websocketID, "ERROR", "error handler triggered" });
                wsClient = null;
                return;
            };
            wsClient.OnClose += (sender, e) =>
            {
                parentFunction("ERROR", packetID++, new object[] { websocketID, "CLOSE", e.Reason });
                wsClient = null;
                return;
            };
            wsClient.OnMessage += (sender, e) =>
            {
                string jsonAsString = e.Data;

                if (initialPacket)
                {
                    initialPacket = false;
                    return;
                }

                int startPosCoin =
                    jsonAsString.IndexOf("product_id", 0) + 13;
                int endPosCoin =
                    jsonAsString.IndexOf("\"", startPosCoin);
                string product =
                    jsonAsString.Substring(startPosCoin, endPosCoin - startPosCoin);

                int startPosSeq =
                    jsonAsString.IndexOf("sequence", 0) + 10;
                int endPosSeq =
                    jsonAsString.IndexOf(",", startPosSeq);
                string sequenceAsString =
                    jsonAsString.Substring(startPosSeq, endPosSeq - startPosSeq);
                UInt64 sequence =
                    Convert.ToUInt64(sequenceAsString);

                parentFunction("MESSAGE", sequence, new object[] { websocketID, product, jsonAsString });
            };

            wsClient.Connect();

            while (wsClient != null && wsClient.IsAlive)
            {
                if (sendQueue[websocketID].Count == 0)
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

            parentFunction("ERROR", packetID++, new object[] { websocketID, "ERROR", "error handler triggered" });
        }

        public void Send(string payload)
        {
            SendQueue[websocketID].Enqueue(payload);
        }

        private static string extractValueFromJson(string input, string key)
        {
            int initialIndex = input.IndexOf(key) + key.Length + 1;
            int finalIndex = input.IndexOf(",", initialIndex);

            if (input.Substring(initialIndex, 2).Equals(":\""))
            {
                initialIndex += 1;
                finalIndex -= 1;
            }

            initialIndex += 1;
            finalIndex -= initialIndex;

            return input.Substring(initialIndex, finalIndex);
        }
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
