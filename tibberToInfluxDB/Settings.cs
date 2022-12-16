using System;
using System.Collections.Generic;

namespace tibberToInfluxDB
{
    internal struct Settings
    {
        internal int influxDBInsertBlockSize;
        internal string influxDBName;
        internal string influxDBPassword;
        internal string influxDBRetentionPolicy;
        internal string influxDBRetentionPolicyLiveMeasurements;
        internal string influxDBServer;
        internal string influxDBTable;
        internal string influxDBTableLiveMeasurements;
        internal string influxDBUser;
        internal LoggingLevel loggingLevel;
        internal string tibberAPItoken;
        internal List<Guid> tibberLiveMeasurementsHomeIDs;
    }
}