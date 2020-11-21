using System;
using System.Collections.Generic;

namespace Websocket_Record_Standard
{
    public class GDAXPacket
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
    public class InboundPacket
    {
        private int websocketID;
        private string packetType;
        private object packetData;

        public InboundPacket()
        {
        }

        public InboundPacket(int websocketID, string packetType)
        {
            this.websocketID = websocketID;
            this.packetType = packetType;
        }

        public InboundPacket(int websocketID, string packetType, object packetData)
        {
            this.websocketID = websocketID;
            this.packetType = packetType;
            this.packetData = packetData;
        }

        public int id {
            get { return websocketID; }
         }
        public string type
        {
            get { return packetType; }
        }
        public object data
        {
            get { return packetData; }
        }
    }
}
