// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Performance.SDK.Processing;
using System;

namespace PerfettoCds.Pipeline.DataOutput
{
    public struct PerfettoGenericEventProjection : IProjection<int, PerfettoGenericEvent>
    {
        private readonly ProcessedEventData<PerfettoGenericEvent> genericEvents;

        public PerfettoGenericEventProjection(ProcessedEventData<PerfettoGenericEvent> genericEvents)
        {
            this.genericEvents = genericEvents;
        }

        public Type SourceType => typeof(int);

        public Type ResultType => typeof(PerfettoGenericEvent);

        public PerfettoGenericEvent this[int value] => this.genericEvents[(uint)value];
    }
}
