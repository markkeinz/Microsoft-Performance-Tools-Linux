using Microsoft.Performance.SDK.Processing;
using PerfettoCds.Pipeline.DataOutput;
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace PerfettoCds.Pipeline
{
    // Theory of operation:
    // --------------------
    // Correlation tries to find related pairs of events in the input event stream
    // and emits new events that can be used to visualize the span formed by identified
    // pairs of events.
    //
    // At the core of the system are correlation rules. Each rule implements an interface
    // that provides a function to examine an event in the context of the rule, and another
    // function to process a pair of events and emit a new event.
    //
    // When examining an event, a rule emits a correlation action and, optionally, an event
    // key. Event keys identify correlation buckets; i.e., only events with equal event keys
    // can be correlated within the scope of a specific rule.
    //
    // Event keys are rule specific; if an event is matched by multiple rules, each rule may
    // produce a different event key. Correlation is only possible within the same rule, but
    // multiple rules may be defined that match the same event and produce spans with different
    // other events.
    //
    // Each rule is wrapped by a correlation context, which manages access to the source and target
    // event streams, and keeps track of the last-seen events for each event key.
    //
    // A default rule implementation is provided whose behavior can be controlled by various
    // parameters passed at creation time. It provides a flexible event key implementation
    // which allows to include a variable subset of event fields.
    //
    // To perform the correlation, the input events are processed in chronological order.
    // Each event is passed to the first event context, which examines the event and, if
    // a correlation has been found, emits a span event. Each context also returns a result
    // whether any further contexts should be allowed to handle the current event. Depending
    // on this result, either the next context (if any) is called with the same event, or the
    // context loop is restarted with the next event.


    // Result returned by correlation rules after examining an event
    enum CorrelationAction
    {
        None, // Rule didn't match
        Ignore, // Rule matched, but indicated to ignore this event
        Push, // Rule indicated to push the current event on the event stack
        Replace, // Rule indicated to replace the top of the event stack with the current event
        PopDiscard, // Rule indicated to remove and discard the top of the event stack
        PopProcess // Rule indicated to process the event from the top of the event stack together with the current event
    }

    // Property of correlation rules indicating under which conditions further rules should be
    // processed for a specific event
    enum StopBehavior
    {
        OnMatch, // Stop processing if the current rule produced a match
        OnAction, // Stop processing if the current rule performed any action
        OnProcess, // Stop processing if the current rule successfully processed an event
        Never // Always continue processing
    }

    // Result returned by a correlation context, indicating whether to continue processing
    // the current event with other correlation contexts.
    enum ProcessingResult
    {
        Stop,
        Continue
    }


    abstract class PerfettoCorrelationRule
    {
        public abstract (CorrelationAction, object) ExamineEvent(in PerfettoGenericEvent e);
        public abstract void ProcessEvent(in PerfettoGenericEvent start, in PerfettoGenericEvent stop, out PerfettoGenericEvent span);
        public abstract StopBehavior StopBehavior { get; }
    };


    class PerfettoCorrelationContext
    {
        public PerfettoCorrelationContext(PerfettoCorrelationRule rule, ProcessedEventData<PerfettoGenericEvent> sourceEvents, ProcessedEventData<PerfettoGenericEvent> eventOutput)
        {
            _rule = rule;
            _sourceEvents = sourceEvents;
            _eventOutput = eventOutput;
        }

        public ProcessingResult ProcessEvent(uint eventIndex)
        {
            PerfettoGenericEvent e = _sourceEvents[eventIndex];
            (CorrelationAction action, object key) = _rule.ExamineEvent(e);
            switch (action)
            {
                case CorrelationAction.None:
                    return ProcessingResult.Continue;
                case CorrelationAction.Ignore:
                    return _rule.StopBehavior > StopBehavior.OnMatch ? ProcessingResult.Continue : ProcessingResult.Stop;
                case CorrelationAction.Push:
                    DoPush(key, eventIndex);
                    return _rule.StopBehavior > StopBehavior.OnAction ? ProcessingResult.Continue : ProcessingResult.Stop;
                case CorrelationAction.Replace:
                    DoReplace(key, eventIndex);
                    return _rule.StopBehavior > StopBehavior.OnAction ? ProcessingResult.Continue : ProcessingResult.Stop;
                case CorrelationAction.PopDiscard:
                    DoPopDiscard(key);
                    return _rule.StopBehavior > StopBehavior.OnAction ? ProcessingResult.Continue : ProcessingResult.Stop;
                case CorrelationAction.PopProcess:
                    if (DoPopProcess(key, eventIndex))
                    {
                        return _rule.StopBehavior > StopBehavior.OnProcess ? ProcessingResult.Continue : ProcessingResult.Stop;
                    }
                    else
                    {
                        return _rule.StopBehavior > StopBehavior.OnMatch ? ProcessingResult.Continue : ProcessingResult.Stop;
                    }
                default:
                    return ProcessingResult.Continue;
            }
        }

        private void DoPush(object key, uint eventIndex)
        {
            if (!_stacks.TryGetValue(key, out Stack<uint> stack))
            {
                stack = new Stack<uint>();
                _stacks[key] = stack;
            }

            stack.Push(eventIndex);
        }

        private void DoReplace(object key, uint eventIndex)
        {
            if (!_stacks.TryGetValue(key, out Stack<uint> stack))
            {
                stack = new Stack<uint>();
                _stacks[key] = stack;
            }

            if (stack.Count > 0)
            {
                _ = stack.Pop();
            }
            stack.Push(eventIndex);
        }

        private void DoPopDiscard(object key)
        {
            if (!_stacks.TryGetValue(key, out Stack<uint> stack))
            {
                return;
            }

            if (stack.Count > 0)
            {
                _ = stack.Pop();
            }

            if (stack.Count == 0)
            {
                _ = _stacks.Remove(key);
            }
        }

        private bool DoPopProcess(object key, uint eventIndex)
        {
            if (!_stacks.TryGetValue(key, out Stack<uint> stack))
            {
                return false;
            }

            if (stack.TryPop(out var prevEventIndex))
            {
                _rule.ProcessEvent(_sourceEvents[prevEventIndex], _sourceEvents[eventIndex], out var span);
                _eventOutput.AddEvent(span);
            }
            else
            {
                return false;
            }

            if (stack.Count > 0)
            {
                stack.Pop();
            }

            if (stack.Count == 0)
            {
                _stacks.Remove(key);
            }

            return true;
        }

        private readonly PerfettoCorrelationRule _rule;
        private readonly ProcessedEventData<PerfettoGenericEvent> _sourceEvents;
        private readonly ProcessedEventData<PerfettoGenericEvent> _eventOutput;
        private readonly Dictionary<object, Stack<uint>> _stacks = new Dictionary<object, Stack<uint>>();
    }


    // Flags indicating fields that can optionally be included in an event key
    [Flags]
    enum OptionalEventKeyFields
    {
        None = 0,
        EventName = 1 << 0,
        OpCode = 1 << 1,
        Process = 1 << 2,
        Thread = 1 << 3
    }

    class ConfigurableCorrelationRule : PerfettoCorrelationRule
    {
        public ConfigurableCorrelationRule(
            String startEventRx, String stopEventRx, String startOpcode, String stopOpcode,
            OptionalEventKeyFields keyFields, bool allowRecursion, StopBehavior stopBehavior,
            List<Tuple<string, string>> additionalEventFields = null)
        {
            if (startEventRx != null)
            {
                _startEventRx = new Regex(startEventRx);
            }
            if (stopEventRx != null)
            {
                _stopEventRx = new Regex(stopEventRx);
            }
            _startOpcode = startOpcode;
            _stopOpcode = stopOpcode;

            if (_startEventRx == null && _startOpcode == null)
            {
                throw new ArgumentException("Search conditions for start event and start opcode cannot both be empty");
            }
            if (_stopEventRx == null && _stopOpcode == null)
            {
                throw new ArgumentException("Search conditions for stop event and stop opcode cannot both be empty");
            }

            _keyFields = keyFields;
            _allowRecursion = allowRecursion;
            _stopBehavior = stopBehavior;
            if (additionalEventFields != null)
            {
                _additionalEventFields = new List<Tuple<string, string>>(additionalEventFields);
            }
        }

        public override (CorrelationAction, Object) ExamineEvent(in PerfettoGenericEvent e)
        {
            if ((_startEventRx == null || _startEventRx.IsMatch(e.EventName)) && (_startOpcode == null || _startOpcode == GetValue(e, "debug.OPCODE")))
            {
                // Found possible start event. Check that any additional fields exist; return if not.
                if (!HasAllFields(e, (t) => t.Item1))
                {
                    return (CorrelationAction.None, null);
                }
                CorrelationAction action = _allowRecursion ? CorrelationAction.Push : CorrelationAction.Replace;
                return (action, MakeKey(e, true));
            }
            else if ((_stopEventRx == null || _stopEventRx.IsMatch(e.EventName)) && (_stopOpcode == null || _stopOpcode == GetValue(e, "debug.OPCODE")))
            {
                // Found possible stop event. Check that any additional fields exist; return if not.
                if (!HasAllFields(e, (t) => t.Item2))
                {
                    return (CorrelationAction.None, null);
                }
                return (CorrelationAction.PopProcess, MakeKey(e, false));
            }
            else
            {
                // No match
                return (CorrelationAction.None, null);
            }
        }

        public override void ProcessEvent(in PerfettoGenericEvent start, in PerfettoGenericEvent stop, out PerfettoGenericEvent span)
        {
            var myDuration = stop.StartTimestamp - start.StartTimestamp;

            span = new PerfettoGenericEvent(StripStartStop(start.EventName), start.Type, myDuration, start.StartTimestamp, stop.StartTimestamp,
                start.Category, start.ArgSetId, start.Values, start.ArgKeys, start.Process, start.Thread);
        }

        public override StopBehavior StopBehavior { get { return _stopBehavior; } }

        private class Key
        {
            private static bool StrEquals(String a, String b)
            {
                return ((a == null) == (b == null)) && (a == null || a.Equals(b));
            }

            public override bool Equals(object obj)
            {
                if (!(obj is Key other))
                {
                    return false;
                }

                if (!StrEquals(EventName, other.EventName)
                    || !StrEquals(Opcode, other.Opcode)
                    || !StrEquals(Process, other.Process)
                    || !StrEquals(Thread, other.Thread))
                {
                    return false;
                }

                if ((Fields == null) != (other.Fields == null))
                {
                    return false;
                }
                if (Fields == null)
                {
                    return true;
                }
                if (Fields.Length != other.Fields.Length)
                {
                    return false;
                }
                for (int i = 0; i < Fields.Length; ++i)
                {
                    if (!StrEquals(Fields[i], other.Fields[i]))
                    {
                        return false;
                    }
                }

                return true;
            }

            public override int GetHashCode()
            {
                int h = 0;

                if (EventName != null)
                {
                    h ^= EventName.GetHashCode();
                }
                if (Opcode != null)
                {
                    h ^= Opcode.GetHashCode();
                }
                if (Process != null)
                {
                    h ^= Process.GetHashCode();
                }
                if (Thread != null)
                {
                    h ^= Thread.GetHashCode();
                }

                if (Fields != null)
                {
                    foreach (var f in Fields)
                    {
                        if (f != null)
                        {
                            h ^= f.GetHashCode();
                        }
                    }
                }

                return h;
            }

            public String EventName;
            public String Opcode;
            public String Process;
            public String Thread;
            public String[] Fields;
        }

        private Key MakeKey(PerfettoGenericEvent e, bool isStartEvent)
        {
            Key k = new Key();
            if ((_keyFields & OptionalEventKeyFields.EventName) != 0)
            {
                k.EventName = StripStartStop(e.EventName);
            }
            if ((_keyFields & OptionalEventKeyFields.OpCode) != 0)
            {
                k.Opcode = GetValue(e, "debug.OPCODE");
            }
            if ((_keyFields & OptionalEventKeyFields.Process) != 0)
            {
                k.Process = e.Process;
            }
            if ((_keyFields & OptionalEventKeyFields.Thread) != 0)
            {
                k.Thread = e.Thread;
            }

            if (_additionalEventFields != null && _additionalEventFields.Count > 0)
            {
                k.Fields = new string[_additionalEventFields.Count];
                for (int i = 0; i < _additionalEventFields.Count; ++i)
                {
                    string fieldName = isStartEvent ? _additionalEventFields[i].Item1 : _additionalEventFields[i].Item2;
                    k.Fields[i] = GetValue(e, fieldName);
                }
            }

            return k;
        }

        private bool HasAllFields(in PerfettoGenericEvent e, Func<Tuple<string, string>, string> selector)
        {
            if (_additionalEventFields != null)
            {
                foreach (Tuple<string, string> t in _additionalEventFields)
                {
                    if (!e.ArgKeys.Contains(selector(t)))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        static string GetValue(in PerfettoGenericEvent e, string k)
        {
            if (e.ArgKeys.Contains(k))
                return e.Values[e.ArgKeys.IndexOf(k)];

            return string.Empty;
        }

        private static readonly string[] _startStopStrings = new string[] { "_Start", "_Stop" };

        private static string StripStartStop(string s)
        {
            foreach (string test in _startStopStrings)
            {
                if (s.EndsWith(test))
                {
                    return s.Substring(0, s.Length - test.Length - 1);
                }
            }

            return s;
        }

        private readonly Regex _startEventRx;
        private readonly Regex _stopEventRx;
        private readonly String _startOpcode;
        private readonly String _stopOpcode;
        private readonly OptionalEventKeyFields _keyFields;
        private readonly bool _allowRecursion;
        private readonly StopBehavior _stopBehavior;
        private readonly List<Tuple<string, string>> _additionalEventFields;
    };
}
