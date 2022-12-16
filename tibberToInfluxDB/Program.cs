using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Mime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Tibber.Sdk;

namespace tibberToInfluxDB
{
    internal static class Program
    {
        public static readonly TimeSpan MaxHistoricalAge = TimeSpan.FromDays(356 * 24 * 5);

        private enum InfluxInsertErrorResult
        {
            Throw,
            Continue,
            RetryWithOneElementAtATime
        }

        /// <summary>
        /// Append something to the output
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="messageLevel"></param>
        /// <param name="message"></param>
        private static void AppendToLog(Settings settings, LoggingLevel messageLevel, string message)
        {
            if (messageLevel == LoggingLevel.None)
                throw new ArgumentException(nameof(messageLevel));

            if (messageLevel > settings.loggingLevel)
                return;

            if (message == null)
                throw new ArgumentNullException(nameof(message));

            Console.WriteLine(message);
        }

        /// <summary>
        /// This will receive the newst datapoint date for all homes form influx. This is used, that
        /// not old data is readded to influx
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private static List<DataPoint> FindLastConsumptionDataPointsInInfluxDB(Settings settings, CancellationHelper continueWork)
        {
            AppendToLog(settings, LoggingLevel.Detailed, "Loading existing datapoints...");

            //Query Data from influx
            string url = GetInfluxDBQueryURL(settings, $"SELECT last( \"cost\") FROM \"{settings.influxDBTable}\" WHERE \"status\"='running' GROUP BY \"homeID\", \"status\" ORDER BY time DESC");
            JToken data;
            using (StreamReader reader = new StreamReader(GetInfluxDBResponse(settings, continueWork, url, WebRequestMethods.Http.Get)))
            {
                if (!continueWork)
                    return null;

                string str = reader.ReadToEnd();
                data = JObject.Parse(str);
            }

            if (!continueWork)
                return null;

            //convert result to datapoints.
            List<DataPoint> ret = new List<DataPoint>();
            data = data["results"][0];
            if (data is JObject obj && obj.ContainsKey("series"))
            {
                foreach (JToken item in data["series"])
                {
                    if (!continueWork)
                        return null;

                    //we only need these informations since nothing else is needed for later comparisons
                    ret.Add(new DataPoint
                    {
                        HomeID = item["tags"].Value<string>("homeID"),
                        Date = item["values"][0][0].Value<DateTime>()
                    });
                }
            }
            else
                AppendToLog(settings, LoggingLevel.Warning, $"Influx repsonse is missing series data! Does the database exist?");

            AppendToLog(settings, LoggingLevel.Detailed, $"Found {ret.Count} homes");
            return ret;
        }

        /// <summary>
        /// This will query the tibber api and will return all datapoints
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="token"></param>
        /// <param name="oldestHomeDate"></param>
        /// <returns></returns>
        private static IEnumerable<DataPoint> GetConsuptionDataFromTibber(Settings settings, CancellationHelper continueWork, DateTime oldestHomeDate)
        {
            AppendToLog(settings, LoggingLevel.Detailed, "Loading data from tibber...");

            //this will be used to limit the number of reuqestet datapoints to whats actually needed
            TimeSpan diffToOldestDate = DateTime.Now - oldestHomeDate;
            if (diffToOldestDate > MaxHistoricalAge)
            {
                diffToOldestDate = MaxHistoricalAge;

                AppendToLog(settings, LoggingLevel.Normal, $"Limiting history to {(MaxHistoricalAge.TotalDays / 356 / 24):f1} years!");
            }
            int numOfElementsToLoad = ((int)diffToOldestDate.TotalHours) + 1; //+1 will add the current hour

            //building request
            HttpWebRequest req = WebRequest.CreateHttp("https://api.tibber.com/v1-beta/gql");
            req.Headers.Set(HttpRequestHeader.Authorization, "Bearer " + settings.tibberAPItoken);
            req.Headers.Set(HttpRequestHeader.ContentType, "application/json; charset=utf-8");
            req.Method = WebRequestMethods.Http.Post;

            using (StreamWriter writer = new StreamWriter(req.GetRequestStream(), Encoding.UTF8))
                writer.Write("{ \"query\": \"{ viewer { homes { id appNickname timeZone consumption(resolution: HOURLY, last: " + numOfElementsToLoad + ", filterEmptyNodes: true) { nodes { to cost unitPrice unitPriceVAT consumption consumptionUnit currency } } currentSubscription { status } } } }\" }");

            //reading response to json
            AppendToLog(settings, LoggingLevel.Detailed, $"Trying to load {numOfElementsToLoad} elements...");
            HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
            JObject obj;
            using (StreamReader reader = new StreamReader(resp.GetResponseStream()))
            {
                string responseString = reader.ReadToEnd();
                obj = JsonConvert.DeserializeObject<JObject>(responseString, new JsonSerializerSettings
                {
                    DateParseHandling = DateParseHandling.DateTimeOffset,
                    DateTimeZoneHandling = DateTimeZoneHandling.Utc
                });
            }

            AppendToLog(settings, LoggingLevel.Detailed, $"Received {resp.ContentLength} bytes of data");

            return ReadTibberResponse(obj);
        }

        /// <summary>
        /// This will create a formatted URL für qerieing/writing to influx
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="submodule"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private static string GetInfluxDBBaseURL(Settings settings, string submodule, List<KeyValuePair<string, string>> args)
        {
            StringBuilder sb = new StringBuilder(512);

            //add http:// to the start of the servers hostname/ip if it is not included
            if (!settings.influxDBServer.StartsWith("HTTP", StringComparison.InvariantCultureIgnoreCase))
                sb.Append("http://");

            sb.Append(settings.influxDBServer);
            sb.Append("/");
            sb.Append(submodule);

            //if no arguemnts where passed
            if (args == null)
                args = new List<KeyValuePair<string, string>>();

            //do formatting on DEBUG
            if (Debugger.IsAttached)
                args.Add(new KeyValuePair<string, string>("pretty", "true"));

            //add Arguments if any
            if (args.Count > 0)
            {
                //uri start of parmeters
                sb.Append("?");

                for (int i = 0; i < args.Count; i++)
                {
                    sb.Append(args[i].Key);
                    sb.Append("=");
                    sb.Append(HttpUtility.UrlEncode(args[i].Value));

                    //seperate every argument by susing &
                    if (i < args.Count - 1)
                        sb.Append("&");
                }
            }

            return sb.ToString();
        }

        /// <summary>
        /// Adds 'db' as a argument to a influx url
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="submodule"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private static string GetInfluxDBDatabaseURL(Settings settings, string submodule, List<KeyValuePair<string, string>> args)
            => GetInfluxDBDatabaseURL(settings, settings.influxDBName, settings.influxDBRetentionPolicy, submodule, args);

        /// <summary>
        /// Adds 'db' as a argument to a influx url
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="influxDBName"></param>
        /// <param name="influxDBRetentionPolicy"></param>
        /// <param name="submodule"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private static string GetInfluxDBDatabaseURL(Settings settings, string influxDBName, string influxDBRetentionPolicy, string submodule, List<KeyValuePair<string, string>> args, string precision = "h")
        {
            Helper.AddOrInitialize(ref args, new KeyValuePair<string, string>("db", settings.influxDBName));
            args.Add(new KeyValuePair<string, string>("precision", precision));

            if (!string.IsNullOrWhiteSpace(influxDBRetentionPolicy))
                args.Add(new KeyValuePair<string, string>("rp", influxDBRetentionPolicy));

            return GetInfluxDBBaseURL(settings, submodule, args);
        }

        /// <summary>
        /// adds a query to a influx url
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="quary"></param>
        /// <returns></returns>
        private static string GetInfluxDBQueryURL(Settings settings, string quary)
        {
            List<KeyValuePair<string, string>> args = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("q", quary)
            };

            return GetInfluxDBDatabaseURL(settings, "query", args);
        }

        /// <summary>
        /// Gets a repsonse from the influx server
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="token"></param>
        /// <param name="url"></param>
        /// <param name="requestMethod"></param>
        /// <param name="writeRequestBody">if null =&gt; no body</param>
        /// <returns></returns>
        private static Stream GetInfluxDBResponse(Settings settings, CancellationHelper continueWork, string url, string requestMethod, Action<Stream> writeRequestBody = null)
        {
            HttpWebRequest req = WebRequest.CreateHttp(url);
            req.Headers.Set(HttpRequestHeader.Authorization, "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(settings.influxDBUser + ":" + settings.influxDBPassword)));
            req.Method = requestMethod;

            //if null do not set a body
            if (writeRequestBody != null)
            {
                using (Stream s = req.GetRequestStream())
                    writeRequestBody(s);
            }

            if (!continueWork)
                return new MemoryStream();

            HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
            return resp.GetResponseStream();
        }

        /// <summary>
        /// this will enumerate over every datapoint and select the newst date
        /// </summary>
        /// <param name="existingData"></param>
        /// <returns></returns>
        private static DateTime GetOldestDateFromDataPoints(IEnumerable<DataPoint> existingData)
        {
            DateTime ret = DateTime.MinValue;

            foreach (DataPoint item in existingData)
            {
                //items date is newer than return date
                if (item.Date > ret)
                    ret = item.Date;
            }

            return ret;
        }

        private static void AddValue(List<string> list, string name, object value)
        {
            if (value == null)
                return;

            string str = string.Concat(name, "=", ToInfluxValueString(value));
            list.Add(str);
        }

        internal class RealTimeMeasurementObserver : IObserver<RealTimeMeasurement>
        {
            private readonly Guid homeID;
            private readonly Settings settings;
            private readonly CancellationHelper continueWork;
            private readonly string url;
            private readonly ConcurrentQueue<RealTimeMeasurement> measurements = new ConcurrentQueue<RealTimeMeasurement>();

            public RealTimeMeasurementObserver(Guid homeID, Settings settings, CancellationHelper continueWork)
            {
                this.homeID = homeID;
                this.settings = settings;
                this.continueWork = continueWork;
                this.url = GetInfluxDBDatabaseURL(settings, settings.influxDBTableLiveMeasurements ?? settings.influxDBTable, settings.influxDBRetentionPolicyLiveMeasurements, "write", null, "s");

                Thread t = new Thread(UploadData);
                t.Start();
            }

            private void UploadData()
            {
                while (!continueWork.IsCanceled)
                {
                    try
                    {
                        if (measurements.Count == 0)
                        {
                            Thread.Sleep(100);
                            continue;
                        }

                        AppendToLog(settings, LoggingLevel.Detailed, "Sending Real Time Measurement...");

                        GetInfluxDBResponse(settings, continueWork, url, WebRequestMethods.Http.Post, stream =>
                        {
                            using (StreamWriter writer = new StreamWriter(stream, encoding: new UTF8Encoding(false, true), leaveOpen: true))
                            {
                                while (measurements.TryDequeue(out RealTimeMeasurement value))
                                {
                                    writer.Write(settings.influxDBTableLiveMeasurements ?? settings.influxDBTable);
                                    writer.Write(",");
                                    WriteDataValue(writer, "homeID", homeID, true);
                                    writer.Write(' ');

                                    List<string> fields = new List<string>();
                                    AddValue(fields, "accumulatedConsumption", value.AccumulatedConsumption);
                                    AddValue(fields, "accumulatedConsumptionLastHour", value.AccumulatedConsumptionLastHour);
                                    AddValue(fields, "accumulatedCost", value.AccumulatedCost);
                                    AddValue(fields, "accumulatedProduction", value.AccumulatedProduction);
                                    AddValue(fields, "accumulatedProductionLastHour", value.AccumulatedProductionLastHour);
                                    AddValue(fields, "accumulatedReward", value.AccumulatedReward);
                                    AddValue(fields, "averagePower", value.AveragePower);
                                    AddValue(fields, "currency", value.Currency);
                                    AddValue(fields, "currentPhase1", value.CurrentPhase1);
                                    AddValue(fields, "currentPhase2", value.CurrentPhase2);
                                    AddValue(fields, "currentPhase3", value.CurrentPhase3);
                                    AddValue(fields, "lastMeterConsumption", value.LastMeterConsumption);
                                    AddValue(fields, "lastMeterProduction", value.LastMeterProduction);
                                    AddValue(fields, "power", value.Power);
                                    AddValue(fields, "powerFactor", value.PowerFactor);
                                    AddValue(fields, "powerReactive", value.PowerReactive);
                                    AddValue(fields, "signalStrength", value.SignalStrength);
                                    AddValue(fields, "voltagePhase1", value.VoltagePhase1);
                                    AddValue(fields, "voltagePhase2", value.VoltagePhase2);
                                    AddValue(fields, "voltagePhase3", value.VoltagePhase3);
                                    string ln = string.Join(',', fields);
                                    writer.Write(ln);

                                    writer.Write(' ');
                                    TimeSpan ts = value.Timestamp.ToUniversalTime() - DateTime.UnixEpoch;
                                    writer.Write((long)ts.TotalSeconds);
                                    writer.Write('\n');
                                }
                            }

                            AppendToLog(settings, LoggingLevel.Detailed, "Waiting for repsonse...");
                        });

                        AppendToLog(settings, LoggingLevel.Detailed, $"Wrote real time data to influx.");
                    }
                    catch (Exception ex)
                    {
                        AppendToLog(settings, LoggingLevel.Error, ex.ToString());
                    }
                }
            }

            public void OnCompleted() => AppendToLog(settings, LoggingLevel.Detailed, "Real time measurement stream has been terminated.");

            public void OnError(Exception error) => AppendToLog(settings, LoggingLevel.Error, error.ToString());

            public void OnNext(RealTimeMeasurement value)
            {
                AppendToLog(settings, LoggingLevel.Detailed, "Received reltime data.");

                measurements.Enqueue(value);
            }
        }

        private static void SubscribeToLiveData(Settings settings, CancellationHelper continueWork)
        {
            if (settings.tibberLiveMeasurementsHomeIDs == null || settings.tibberLiveMeasurementsHomeIDs.Count == 0)
                return;

            ProductInfoHeaderValue userAgent = new ProductInfoHeaderValue("tibberflux", "0.0.1");
            TibberApiClient client = new TibberApiClient(settings.tibberAPItoken, userAgent);

            foreach (Guid homeID in settings.tibberLiveMeasurementsHomeIDs)
            {
                IObservable<RealTimeMeasurement> listener = client.StartRealTimeMeasurementListener(homeID, cancellationToken: continueWork.CancellationToken).Result;
                listener.Subscribe(new RealTimeMeasurementObserver(homeID, settings, continueWork));
            }
        }

        /// <summary>
        /// Main entry
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private static int Main(string[] args)
        {
            //Parse command line args
            Settings? settingsN = ParseCommandLineArgs(args);
            if (settingsN == null)
            {
                //print help if arguments are wrong
                PrintHelp();

                return 1;
            }

            Settings settings = settingsN.Value;

            //for using ctrl+c
            CancellationHelper continueWork = new CancellationHelper();
            Console.CancelKeyPress += (s, e) =>
            {
                if (continueWork)
                    AppendToLog(settings, LoggingLevel.Error, "Received request to canel...");
                else
                    AppendToLog(settings, LoggingLevel.Error, "Still waiting for clean exit...");

                continueWork.Cancel();

                e.Cancel = true;
            };

            SubscribeToLiveData(settings, continueWork);

            //main loop body
            while (continueWork)
            {
                //just for a later display
                DateTime loopStart = DateTime.Now;
                AppendToLog(settings, LoggingLevel.Normal, "Staring loop...");
                bool hadError = false;

                try
                {
                    MainAppLoop(settings, continueWork);
                }
                catch (Exception ex)
                {
                    hadError = true;

                    AppendToLog(settings, LoggingLevel.Error, ex.ToString());

                    if (ex is WebException wex && wex.Response is HttpWebResponse resp)
                    {
                        using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
                        {
                            string s = sr.ReadToEnd();
                            AppendToLog(settings, LoggingLevel.Error, s);
                        }
                    }
                }

                //show loop duration
                TimeSpan duration = DateTime.Now - loopStart;
                AppendToLog(settings, LoggingLevel.Normal, $"Finished run after {duration.TotalSeconds:f0} seconds.");

                //sleep delay
                TimeSpan delay;
                if (hadError)
                    delay = TimeSpan.FromSeconds(10);
                else
                    delay = TimeSpan.FromMinutes(15);

                AppendToLog(settings, LoggingLevel.Detailed, $"Next run in {delay.TotalSeconds:f0} seconds.");

                //actual delay
                continueWork.Wait(delay);
            }

            AppendToLog(settings, LoggingLevel.Detailed, "Exitting...");

            return 0;
        }

        /// <summary>
        /// This the the main app body. It will be called in a loop
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="token"></param>
        private static void MainAppLoop(Settings settings, CancellationHelper continueWork)
        {
            //1. Load existing data pomits from InfluxDB (only the last point is acutally tranfered to this application)
            List<DataPoint> existingData = FindLastConsumptionDataPointsInInfluxDB(settings, continueWork);
            if (!continueWork)
                return;

            //2. Get oldest newest datapoint. This will define how much date we actuall need to query
            DateTime oldestHomeDate = GetOldestDateFromDataPoints(existingData);
            if (!continueWork)
                return;

            //3. Load consumption data from tibber. This will use a 'lazy' enumerator for conversion from json to a object. This will reduce the amount of memory needed by this app.
            IEnumerable<DataPoint> data = GetConsuptionDataFromTibber(settings, continueWork, oldestHomeDate);
            if (!continueWork)
                return;

            //4. Filter the data from tibber using a 'lazy' enumerable
            data = RemoveOldDataPointsFromTibberResults(settings, continueWork, data, existingData);

            //5. Write data to influx
            WriteDataToInfluxDB(settings, continueWork, data);
        }

        /// <summary>
        /// Parse command line args
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private static Settings? ParseCommandLineArgs(string[] args)
        {
            //Default settings
            Settings ret = new Settings
            {
                influxDBTable = "tibberflux",
                influxDBTableLiveMeasurements = "tibberfluxlive",
                influxDBInsertBlockSize = 10,
                loggingLevel = LoggingLevel.Normal
            };

            //predefined args
            if (!args.TryGet(0, out ret.tibberAPItoken))
                return null;

            if (!args.TryGet(1, out ret.influxDBServer))
                return null;

            if (!args.TryGet(2, out ret.influxDBName))
                return null;

            if (!args.TryGet(3, out ret.influxDBUser))
                return null;

            if (!args.TryGet(4, out ret.influxDBPassword))
                return null;

            //for non predefined args
            for (int i = 5; i < args.Length; i++)
            {
                switch (args[i].ToUpper())
                {
                    //table
                    case "--TABLE":
                        if (args.TryGet(++i, out ret.influxDBTable))
                            ret.influxDBTable = ret.influxDBTable.Trim();
                        break;

                    //table for real time data
                    case "--TABLERT":
                        if (args.TryGet(++i, out ret.influxDBTableLiveMeasurements))
                            ret.influxDBTableLiveMeasurements = ret.influxDBTableLiveMeasurements.Trim();
                        break;

                    //logging
                    case "--LOGGING":
                        if (!args.TryGet(++i, out string loggingLevelStr))
                            break;

                        //all avalible levels
                        LoggingLevel[] avalibleLevels = (LoggingLevel[])Enum.GetValues(typeof(LoggingLevel));

                        //i dont want to write it twice this is why is included here.
                        bool CompareLLToValueAndSet(Predicate<LoggingLevel> compare)
                        {
                            for (int j = 0; j < avalibleLevels.Length; j++)
                            {
                                if (compare(avalibleLevels[j]))
                                {
                                    ret.loggingLevel = avalibleLevels[j];

                                    return true;
                                }
                            }

                            AppendToLog(ret, LoggingLevel.Error, $"Can not convert '{loggingLevelStr}' to a LoggingLevel!");

                            return false;
                        }

                        if (int.TryParse(loggingLevelStr, out int lli))//levels by number
                            CompareLLToValueAndSet(l => (int)l == lli);
                        else //levels by name or anything else
                        {
                            loggingLevelStr = loggingLevelStr.ToUpper();
                            CompareLLToValueAndSet(l => l.ToString().ToUpper() == loggingLevelStr);
                        }

                        break;

                    //block size for influx db writes
                    case "--INSERTBLOCKSIZE":
                        if (args.TryGet(++i, out string s))
                            int.TryParse(s, out ret.influxDBInsertBlockSize);
                        break;

                    //Retention Policy
                    case "--RETENTIONPOLICY":
                        args.TryGet(++i, out ret.influxDBRetentionPolicy);
                        break;

                    //Retention Policy
                    case "--RTHOMES":
                        if (!args.TryGet(++i, out string homeguids))
                            break;

                        ret.tibberLiveMeasurementsHomeIDs = new List<Guid>();
                        foreach (string guidstr in homeguids.Split(',', StringSplitOptions.None))
                        {
                            if (Guid.TryParse(guidstr, out Guid homeid))
                                ret.tibberLiveMeasurementsHomeIDs.Add(homeid);
                        }
                        break;


                    //Retention Policy
                    case "--RETENTIONPOLICYRT":
                        args.TryGet(++i, out ret.influxDBRetentionPolicyLiveMeasurements);
                        break;

                    //fallback
                    default:
                        AppendToLog(ret, LoggingLevel.Error, $"Argument {args[i]} unknown!");
                        break;
                }
            }

            return ret;
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Arguments: <tibberAPIToken> <InfluxDBServer> <InfluxDBDatabase> <InfluxDBUser> <InfluxDBPassword>");
            Console.WriteLine();
            Console.WriteLine("--table: Target table for InfluxDB");
            Console.WriteLine("--tablert: Target table for InfluxDB with RealTime Measurements");
            Console.WriteLine("--rthomes: GUIDs of Homes for live measurements. Delimiter: ','");
            Console.WriteLine("--retentionpolicyrt: GUIDs of Homes for live measurements. Delimiter: ','");
        }

        /// <summary>
        /// this will actually convert the received json to a datapoint
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private static IEnumerable<DataPoint> ReadTibberResponse(JObject obj)
        {
            //for every home
            foreach (JToken item in obj["data"]["viewer"]["homes"])
            {
                string id = item.Value<string>("id");
                string appNickname = item.Value<string>("appNickname");
                string status = item["currentSubscription"].Value<string>("status");

                //for every consumption node
                foreach (JToken node in item["consumption"]["nodes"])
                {
                    DateTimeOffset dateLocal = node.Value<DateTimeOffset>("to"); //stupid timezones
                    DateTime date = new DateTime(dateLocal.DateTime.Ticks - (dateLocal.Offset.Ticks * -1), DateTimeKind.Utc);

                    //convert consumption to Wh
                    float consumption = node.Value<float>("consumption");
                    string consumptionUnit = node.Value<string>("consumptionUnit");
                    if (consumptionUnit == "kWh")
                        consumption = consumption * 1000;

                    DataPoint point = new DataPoint
                    {
                        ConsumptionWatt = consumption,
                        Cost = node.Value<float>("cost"),
                        Currency = node.Value<string>("currency"),
                        Date = date,
                        HomeID = id,
                        HomeName = appNickname,
                        Status = status,
                        UnitPrice = node.Value<float>("unitPrice"),
                        UnitPriceVAT = node.Value<float>("unitPriceVAT")
                    };

                    yield return point;
                }
            }
        }

        /// <summary>
        /// This will append another yield based filter fro lower memeory consumption
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="token"></param>
        /// <param name="data"></param>
        /// <param name="lastDataPoints"></param>
        /// <returns></returns>
        private static IEnumerable<DataPoint> RemoveOldDataPointsFromTibberResults(Settings settings, CancellationHelper continueWork, IEnumerable<DataPoint> data, List<DataPoint> lastDataPoints)
        {
            ///for every point
            foreach (DataPoint item in data)
            {
                //if a date newer than this is already defined skip it and move on
                bool pointExists = false;
                for (int i = 0; i < lastDataPoints.Count; i++)
                {
                    if (lastDataPoints[i].HomeID == item.HomeID) //compare honeID
                    {
                        if (lastDataPoints[i].Date > item.Date) //compare Dates
                        {
                            pointExists = true;
                            break;
                        }
                    }
                }
                if (pointExists) //if it is already present skip this point and move to the next one
                    continue;

                yield return item;
            }
        }

        /// <summary>
        /// This will handle some of the most common errors thrown by influx.
        /// </summary>
        /// <param name="settings"></param>
        /// <param name="ex"></param>
        /// <param name="elementsInblock"></param>
        /// <returns></returns>
        private static InfluxInsertErrorResult TryHandleInfluxDBInsertError(Settings settings, Exception ex, int elementsInblock)
        {
            if (!(ex is WebException wex)) //not related to server errors
                return InfluxInsertErrorResult.Throw;

            if (!(wex.Response is HttpWebResponse resp)) //not related to a response
                return InfluxInsertErrorResult.Throw;

            if (!resp.ContentType.StartsWith(MediaTypeNames.Application.Json)) //message content is not a json object
                return InfluxInsertErrorResult.Throw;

            //read message body
            JObject data;
            using (StreamReader sr = new StreamReader(resp.GetResponseStream()))
            {
                string s = sr.ReadToEnd();
                data = JObject.Parse(s);
            }

            string errorMessage = data.Value<string>("error");
            if (errorMessage == null)
                return InfluxInsertErrorResult.Throw;

            //handeling for parital write
            const string errorMessagePartialWrite = "partial write: points beyond retention policy dropped=";
            if (errorMessage.StartsWith(errorMessagePartialWrite))
            {
                errorMessage = errorMessage.Substring(errorMessagePartialWrite.Length);

                if (int.TryParse(errorMessage, out int numberOfFailedInserts))
                {
                    if (elementsInblock != 1)
                        AppendToLog(settings, LoggingLevel.Warning, $"Send {elementsInblock} to database but only {elementsInblock - numberOfFailedInserts} where accepted and {numberOfFailedInserts} dropped due to a retention policy");

                    if (numberOfFailedInserts == elementsInblock)
                        return InfluxInsertErrorResult.Continue;
                    else
                        return InfluxInsertErrorResult.RetryWithOneElementAtATime;
                }
                else
                    AppendToLog(settings, LoggingLevel.Warning, errorMessagePartialWrite + errorMessage);

                return InfluxInsertErrorResult.Throw;
            }

            //message not handeled!
            return InfluxInsertErrorResult.Throw;
        }

        private static void WriteBlockToDatabase(Settings settings, CancellationHelper continueWork, string url, List<DataPoint> dataPoints)
        {
            GetInfluxDBResponse(settings, continueWork, url, WebRequestMethods.Http.Post, stream =>
            {
                using (StreamWriter writer = new StreamWriter(stream, encoding: new UTF8Encoding(false, true), leaveOpen: true))
                {
                    foreach (DataPoint item in dataPoints)
                    {
                        if (!continueWork)
                            return;

                        writer.Write(settings.influxDBTable);
                        writer.Write(",");

                        WriteDataValue(writer, "homeID", item.HomeID, true);
                        writer.Write(',');
                        WriteDataValue(writer, "homeAppName", item.HomeName, true);
                        writer.Write(',');
                        WriteDataValue(writer, "status", item.Status, true);

                        writer.Write(' ');
                        WriteDataValue(writer, "consumptionWatts", item.ConsumptionWatt);
                        writer.Write(',');
                        WriteDataValue(writer, "cost", item.Cost);
                        writer.Write(',');
                        WriteDataValue(writer, "currency", item.Currency);
                        writer.Write(',');
                        WriteDataValue(writer, "unitPrice", item.UnitPrice);
                        writer.Write(',');
                        WriteDataValue(writer, "unitVatPrice", item.UnitPriceVAT);
                        writer.Write(' ');

                        TimeSpan ts = item.Date.ToUniversalTime() - DateTime.UnixEpoch;
                        writer.Write((int)ts.TotalHours);
                        writer.Write('\n');
                    }
                }

                AppendToLog(settings, LoggingLevel.Detailed, "Waiting for repsonse...");
            });

            AppendToLog(settings, LoggingLevel.Detailed, $"Wrote {dataPoints.Count} rows of data to influx");
        }

        private static void WriteDataToInfluxDB(Settings settings, CancellationHelper continueWork, IEnumerable<DataPoint> dataPoints)
        {
            string url = GetInfluxDBDatabaseURL(settings, "write", null);
            IEnumerator<DataPoint> enumerator = dataPoints.GetEnumerator();
            int totalNumberOfInsertions = 0;
            while (!!continueWork)
            {
                AppendToLog(settings, LoggingLevel.Detailed, "Writing next block of data to influx...");

                bool insertSingleElements = false;
                int blockIndex = 0;

                List<DataPoint> block = new List<DataPoint>();
                for (int i = 0; i < settings.influxDBInsertBlockSize; i++) //try to add a maximum of settings.influxDBInsertBlockSizeelements or untill the end of the list
                {
                    if (!enumerator.MoveNext())
                        break;

                    block.Add(enumerator.Current);
                }
                if (block.Count == 0)
                    break;

                //This loop will first try to add all elements in one go. If this fails it will try to add one element at a time
                while (!!continueWork)
                {
                    try
                    {
                        List<DataPoint> subList;
                        if (insertSingleElements)
                        {
                            subList = new List<DataPoint>();

                            if (blockIndex > block.Count)
                                break;

                            subList.Add(block[blockIndex]);

                            blockIndex++;
                        }
                        else
                            subList = block;

                        WriteBlockToDatabase(settings, continueWork, url, subList);
                        totalNumberOfInsertions = totalNumberOfInsertions + subList.Count;

                        break;
                    }
                    catch (Exception ex)
                    {
                        int numberOfElementsInCurrentBlock = insertSingleElements ? 1 : block.Count;

                        InfluxInsertErrorResult handleResult = TryHandleInfluxDBInsertError(settings, ex, numberOfElementsInCurrentBlock);

                        if (handleResult == InfluxInsertErrorResult.RetryWithOneElementAtATime) //Inserting multiple failed. Trying to add one element at a time
                        {
                            AppendToLog(settings, LoggingLevel.Normal, "Retrying current Datablock with 1 element at a time.");

                            insertSingleElements = true;

                            continue;
                        }

                        if (handleResult == InfluxInsertErrorResult.Continue) //Error is okay
                        {
                            if (insertSingleElements) //if one element is inserted at a time the loop will continue
                                continue;
                            else
                                break;
                        }

                        throw;
                    }
                }
            }

            AppendToLog(settings, LoggingLevel.Normal, $"Inserted {totalNumberOfInsertions} new points to InfluxDB");
        }

        private static string ToInfluxValueString(object value, bool isTag = false)
        {
            bool doNotEscapeFirstAndLastChar = false;

            string str;
            if (value is float f)
                str = f.ToString(CultureInfo.InvariantCulture);
            else if (value is double d)
                str = d.ToString(CultureInfo.InvariantCulture);
            else if (value is decimal dc)
                str = dc.ToString(CultureInfo.InvariantCulture);
            else
            {
                str = value.ToString();

                if (!isTag)
                {
                    str = "\"" + str + "\"";
                    doNotEscapeFirstAndLastChar = true;
                }
            }

            StringBuilder sb = new StringBuilder(str.Length * 2);
            for (int i = 0; i < str.Length; i++)
            {
                if (!doNotEscapeFirstAndLastChar || (doNotEscapeFirstAndLastChar && i != 0 && i != str.Length - 1))
                {
                    if (str[i] == ',' || str[i] == ' ' || str[i] == '=')
                        sb.Append("\\");
                }

                sb.Append(str[i]);
            }

            return sb.ToString();
        }

        private static bool WriteDataValue(StreamWriter writer, string key, object value, bool isTag = false)
        {
            if (!isTag && value == null)
                return false;

            writer.Write(key);
            writer.Write('=');

            string str = ToInfluxValueString(value, isTag);
            writer.Write(str);

            return true;
        }
    }
}