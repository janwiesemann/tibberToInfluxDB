namespace tibberToInfluxDB
{
    internal struct Settings
    {
        internal int influxDBInsertBlockSize;
        internal string influxDBName;
        internal string influxDBPassword;
        internal string influxDBRetentionPolicy;
        internal string influxDBServer;
        internal string influxDBTable;
        internal string influxDBUser;
        internal LoggingLevel loggingLevel;
        internal string tibberAPItoken;
    }
}