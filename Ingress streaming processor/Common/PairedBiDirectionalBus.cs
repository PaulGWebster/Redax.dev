using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace ConcurrentMessageBus
{
    public class PairedBiDirectionalBus
    {
        List<BlockingCollection<BiDireactionalBusPacket>> queues = new List<BlockingCollection<BiDireactionalBusPacket>>();

        private int senderSide = 0;
        public int Register()
        {
            if (senderSide > 1)
            {
                throw new Exception("Only 2 processes may use this bus type.");
            }
            Console.WriteLine("Peer {0} registered.", senderSide);
            queues.Add(new BlockingCollection<BiDireactionalBusPacket>() { new BiDireactionalBusPacket(0,"Registered",null) });
            return senderSide++;
        }
        public int pairPartner(int myId)
        {
            if (myId == 0) { return 1; }
            else { return 0; }
        }
        public BiDireactionalBusPacket Pull(int senderQueue)
        {
            BiDireactionalBusPacket returnPacket = null;
            queues[senderQueue].TryTake(out returnPacket, 1);
            return returnPacket;
        }
        public int Push(
            int targetQueue,
            int Type, 
            object Data, 
            string Stash
        )
        {
            BiDireactionalBusPacket packet = new BiDireactionalBusPacket(Type, Data, Stash);
            queues[targetQueue].Add(packet);
            return queues[targetQueue].Count;
        }
    }
    public class BiDireactionalBusPacket
    {
        public BiDireactionalBusPacket(int Type,object Data,string Stash)
        {
            this.Type = Type;
            this.Data = Data;
            this.Stash = Stash;
        }
        public int Type { get; private set; }
        public object Data { get; private set; }
        public string Stash { get; private set; }
    }
}
