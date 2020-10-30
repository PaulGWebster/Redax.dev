using System;
using System.Collections.Generic;
using System.Threading;

using ConcurrentMessageBus;
using WebSocketSharp;

namespace WebsocketManager
{
    public class websocketInterface
    {
        private websocketContainer wsContainer = new websocketContainer();
  
        public void New(string URI, string Tag, PairedBiDirectionalBus Queue)
        {
            wsContainer.New(URI, Tag, Queue);
        }

        public void Start(string tag) {
            wsContainer.Start(tag);
        }
    }
    internal class websocketContainer
    {
        private Dictionary<string, websocketProcess> wsProcesses = 
            new Dictionary<string, websocketProcess>();

        internal void New(string URI, string tag, PairedBiDirectionalBus queue)
        {
            websocketProcess newWorker = 
                new websocketProcess(URI, tag, queue);
            wsProcesses.Add(tag, newWorker);
        }

        internal void Start(string tag)
        {
            wsProcesses[tag].Start();
        }
    }

    internal class websocketProcess
    {
        ParameterizedThreadStart threadStart = 
            new ParameterizedThreadStart(websocketProcessFunction);
        Thread threadObject;

        string URI;
        string tag;

        PairedBiDirectionalBus queue;

        public websocketProcess(string URI, string tag, PairedBiDirectionalBus queue)
        {
            this.URI = URI;
            this.tag = tag;
            this.queue = queue;
            threadObject = new Thread(threadStart);
        }
        
        public void Start()
        {
            threadObject.Start(new object[] { queue, URI, tag });
        }

        private static void websocketProcessFunction(object passedObject)
        {
            object[] passedArgs = (object[])passedObject;

            PairedBiDirectionalBus queue = (PairedBiDirectionalBus)passedArgs[0];
            string URI = (string)passedArgs[1];
            string tag = (string)passedArgs[2];

            int senderQueue = queue.Register();

            WebSocket websocketObj = new WebSocket(URI);
            websocketObj.SslConfiguration.EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12;
            websocketObj.EmitOnPing = false;
            websocketObj.OnOpen += (sender, e) =>
            {
                queue.Push(queue.pairPartner(senderQueue), 10, "Websocket open", tag);
            };
            websocketObj.OnError += (sender, e) =>
            {
                queue.Push(queue.pairPartner(senderQueue), 11, "Websocket error", tag);
            };
            websocketObj.OnClose += (sender, e) =>
            {
                queue.Push(queue.pairPartner(senderQueue), 12, "Websocket closed", tag);
            };
            websocketObj.OnMessage += (sender, e) =>
            {
                queue.Push(queue.pairPartner(senderQueue), 13, e.Data, tag);
            };
            websocketObj.Connect();

            while (true)
            {
                Thread.Sleep(6000);
                BiDireactionalBusPacket packet = queue.Pull(senderQueue);
                if (packet == null) { continue; }
                if (packet.Type == 0)
                {
                    // Welcome message ... ignore it
                }
                else if (packet.Type == 20)
                {
                    websocketObj.Send((string)packet.Data);
                }
                else
                {
                    throw new Exception(string.Format("Do not know what {0} as a type is or howto handle it.", packet.Type));
                }
            }
        }
    }
}
