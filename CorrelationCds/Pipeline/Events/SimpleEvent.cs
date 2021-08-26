using Microsoft.Performance.SDK;
using System;
using System.Collections.Generic;
using System.Text;

namespace CorrelationCds.Pipeline.Events
{
    public sealed class SimpleEvent
    {
        private static uint nextId = 1;

        public uint Id;
        public Timestamp Time;
        public string Name;

        public SimpleEvent(Timestamp time, string name)
        {
            this.Id = nextId++;
            this.Time = time;
            this.Name = name;
        }
    }
}
