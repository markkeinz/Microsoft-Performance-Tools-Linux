// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.Performance.SDK.Extensibility;
using Microsoft.Performance.SDK.Extensibility.DataCooking;
using Microsoft.Performance.SDK.Processing;
using PerfettoCds.Pipeline.DataOutput;
using System;
using System.Collections.Generic;

namespace PerfettoCds.Pipeline.DataCookers
{

    /// <summary>
    /// Pulls data from the Perfetto Generic Event cooker and tries to match events that form spans.
    /// </summary>
    public sealed class PerfettoEventCorrelationCooker : CookedDataReflector, ICompositeDataCookerDescriptor
    {
        public static readonly DataCookerPath DataCookerPath = PerfettoPluginConstants.EventCorrelationCookerPath;

        public string Description => "Event correlation composite cooker";

        public DataCookerPath Path => DataCookerPath;

        // Declare all of the cookers that are used by this CompositeCooker.
        public IReadOnlyCollection<DataCookerPath> RequiredDataCookers => new[]
        {
            PerfettoPluginConstants.GenericEventCookerPath
        };

        /// <summary>
        /// The highest number of fields found in any single event.
        /// </summary>
        [DataOutput]
        public int MaximumEventFieldCount { get; private set; }

        [DataOutput]
        public ProcessedEventData<PerfettoGenericEvent> CorrelatedEvents { get; }

        public PerfettoEventCorrelationCooker() : base(PerfettoPluginConstants.EventCorrelationCookerPath)
        {
            this.CorrelatedEvents =
                new ProcessedEventData<PerfettoGenericEvent>();
        }

        private static readonly List<PerfettoCorrelationRule> correlationRules = new List<PerfettoCorrelationRule>()
        {
            new ConfigurableCorrelationRule(
                "StreamingVideoProducer_BeginFrame", "StreamingVideoProducer_EndFrame", null, null,
                OptionalEventKeyFields.Process|OptionalEventKeyFields.Thread,
                false, StopBehavior.OnAction,
                new List<Tuple< string, string >>(){ new Tuple<string, string>("debug.farmeId", "debug.frameId") }),

            new ConfigurableCorrelationRule(
                null, null, "1", "2",
                OptionalEventKeyFields.EventName|OptionalEventKeyFields.Process|OptionalEventKeyFields.Thread,
                true, StopBehavior.OnProcess),

            new ConfigurableCorrelationRule(
                null, null, "1", "2",
                OptionalEventKeyFields.EventName|OptionalEventKeyFields.Process,
                true, StopBehavior.OnProcess,
                new List<Tuple<string, string>>(){ new Tuple<string, string>("debug.ActivityId", "debug.ActivityId") }),
        };

        public void OnDataAvailable(IDataExtensionRetrieval requiredData)
        {
            MaximumEventFieldCount = requiredData.QueryOutput<int>(
                new DataOutputPath(PerfettoPluginConstants.GenericEventCookerPath, nameof(PerfettoGenericEventCooker.MaximumEventFieldCount)));

            ProcessedEventData<PerfettoGenericEvent> eventData = requiredData.QueryOutput<ProcessedEventData<PerfettoGenericEvent>>(
                new DataOutputPath(PerfettoPluginConstants.GenericEventCookerPath, nameof(PerfettoGenericEventCooker.GenericEvents)));

            List<PerfettoCorrelationContext> contexts = new List<PerfettoCorrelationContext>();
            foreach (var rule in correlationRules)
            {
                contexts.Add(new PerfettoCorrelationContext(rule, eventData, CorrelatedEvents));
            }

            for (uint eventIndex = 0; eventIndex < eventData.Count; ++eventIndex)
            {
                foreach (var c in contexts)
                {
                    if (c.ProcessEvent(eventIndex) != ProcessingResult.Continue)
                    {
                        break;
                    }
                }
            }

            CorrelatedEvents.FinalizeData();
        }
    }
}
