using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

using System.Configuration;
using Common;

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
            // Start websocket connectors - not saving packets, pass reference back to a function on main
            websocketInterface wsCtrl = new websocketInterface();

            // "wss://ws-feed.pro.coinbase.com"

        }
    }
}
