using CorrelationCds.Pipeline.Events;
using Microsoft.Performance.Core4.Interop.PerfCore4;
using Microsoft.Performance.SDK;
using Microsoft.Performance.SDK.Extensibility;
using Microsoft.Performance.SDK.Processing;
using Microsoft.Windows.EventTracing;
using Microsoft.Windows.EventTracing.Events;
using System;
using System.Collections.Generic;
using System.Text;
using XPerfCore;
using Timestamp = Microsoft.Performance.SDK.Timestamp;

namespace CorrelationCds.Pipeline.Tables
{
    public static class StringFlyweightExtensions
    {
        public static unsafe NativeString ToGraphable(this StringFlyweight flyweight)
        {
            return new NativeString(flyweight.NativePointer);
        }

        public static string ToManagedString(this StringFlyweight flyweight)
        {
            return flyweight.ToGraphable().ToString();
        }

        public static string ToManagedStringOrEmpty(this StringFlyweight flyweight)
        {
            return flyweight.HasValue ? flyweight.ToGraphable().ToString() : string.Empty;
        }
    }

    [Table]
    public sealed class SimpleEventTable
    {
        private static DataCookerPath SourceCookerPath =
            new DataCookerPath(XPerfConstants.XPerfSourceParserId, "DLCookerGenericEvents");

        public static TableDescriptor TableDescriptor => new TableDescriptor(
            Guid.Parse("{5206E81E-644C-48B4-8819-32CC2DB2E4D7}"),
            "SimpleEvents",
            "Simple events",
            "Correlation",
            requiredDataCookers: new List<DataCookerPath> { SourceCookerPath }
        );

        private static readonly ColumnConfiguration IdColumn = new ColumnConfiguration(
            new ColumnMetadata(new Guid("{E72BE547-6E2F-49C8-9781-FB590428B7CE}"), "ID", "ID of the event"),
            new UIHints { Width = 80 });

        private static readonly ColumnConfiguration TimestampColumn = new ColumnConfiguration(
            new ColumnMetadata(new Guid("{C72C543D-5BD0-4082-93FC-124AD909547F}"), "Timestamp", "Timestamp for the event"),
            new UIHints { Width = 120 });

        private static readonly ColumnConfiguration EventNameColumn = new ColumnConfiguration(
            new ColumnMetadata(new Guid("{91DCBF6C-926A-4D2B-B9DD-4481E4DCB199}"), "Name", "Name of the event"),
            new UIHints { Width = 210 });

        public static void BuildTable(ITableBuilder tableBuilder, IDataExtensionRetrieval tableData)
        {
            // Get data from the cooker
            var events = tableData.QueryOutput<IReadOnlyList<GenericEventFlyweight>>(
                new DataOutputPath(SourceCookerPath, "GenericEvents"));

            var eventContext = tableData.QueryOutput<IGenericEventContext>(
                new DataOutputPath(SourceCookerPath, "GenericEventContext"));

            var timestampContext = tableData.QueryOutput<ITraceTimestampContext>(
                new DataOutputPath(SourceCookerPath, "TraceTimestampContext"));

            // Start construction of the column order. Pivot on process and thread
            List<ColumnConfiguration> allColumns = new List<ColumnConfiguration>()
            {
                IdColumn,
                TableConfiguration.PivotColumn, // Columns before this get pivoted on
                EventNameColumn,
                TableConfiguration.GraphColumn, // Columns after this get graphed
                TimestampColumn,
            };

            var tableGenerator = tableBuilder.SetRowCount(events.Count);
            var eventProjection = new GenericEventProjection(events);

            var idColumn = new BaseDataColumn<int>(
                IdColumn,
                eventProjection.Compose((e) => e.Id));
            tableGenerator.AddColumn(idColumn);

            var eventNameColumn = new BaseDataColumn<string>(
                EventNameColumn,
                eventProjection.Compose((e) => e.GetMetadata(eventContext).TaskName.ToManagedStringOrEmpty()));
            tableGenerator.AddColumn(eventNameColumn);

            var timestampColumn = new BaseDataColumn<Timestamp>(
                TimestampColumn,
                eventProjection.Compose((e) => new Timestamp(new TraceTimestamp(timestampContext, e.Timestamp).Nanoseconds)));
            tableGenerator.AddColumn(timestampColumn);

            var tableConfig = new TableConfiguration("Simple Events")
            {
                Columns = allColumns,
                Layout = TableLayoutStyle.GraphAndTable
            };
            tableConfig.AddColumnRole(ColumnRole.StartTime, TimestampColumn.Metadata.Guid);

            tableBuilder.AddTableConfiguration(tableConfig).SetDefaultTableConfiguration(tableConfig);
        }
    }

    public struct GenericEventProjection : IProjection<int, GenericEventFlyweight>
    {
        private readonly IReadOnlyList<GenericEventFlyweight> events;

        public GenericEventProjection(IReadOnlyList<GenericEventFlyweight> events)
        {
            this.events = events;
        }

        public Type SourceType => typeof(int);

        public Type ResultType => typeof(GenericEventFlyweight);

        public GenericEventFlyweight this[int value] => this.events[value];
    }

    public struct SimpleEventProjection : IProjection<int, SimpleEvent>
    {
        private readonly ProcessedEventData<SimpleEvent> events;

        public SimpleEventProjection(ProcessedEventData<SimpleEvent> events)
        {
            this.events = events;
        }

        public Type SourceType => typeof(int);

        public Type ResultType => typeof(SimpleEvent);

        public SimpleEvent this[int value] => this.events[(uint)value];
    }
}
