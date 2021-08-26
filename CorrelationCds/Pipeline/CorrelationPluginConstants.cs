using Microsoft.Performance.SDK.Extensibility;
using System;
using System.Collections.Generic;
using System.Text;

namespace CorrelationCds.Pipeline
{
    public static class CorrelationPluginConstants
    {
        public const string ParserId = "CorrelationSourceParser";

        public const string CombiningCorrelationCookerId = "CombiningCorrelationCooker";
        public const string XperfEventPreCookerId = "XperfEventPreCooker";

        public static readonly DataCookerPath CombiningCorrelationCookerPath =
            new DataCookerPath(CombiningCorrelationCookerId);
        public static readonly DataCookerPath XperfEventPreCookerPath =
            new DataCookerPath(XperfEventPreCookerId);
    }
}
