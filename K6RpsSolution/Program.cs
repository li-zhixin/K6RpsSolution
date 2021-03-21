using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using MathNet.Numerics;
using ServiceStack.Text;
using XPlot.Plotly;
using InfluxDB.Client;
using InfluxDB.Client.Core.Flux.Domain;
using static ServiceStack.Text.Common.DateTimeSerializer;

namespace K6RpsSolution
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var rpsTrace = GetTraceFromJson(args, out var fitTrace, out var vusTrace, out var startTime,
                out var endTime);
            var chart = Chart.WithLayout(new Layout.Layout()
            {
                yaxis2 = new Yaxis()
                {
                    title = "request/s",
                    side = "right",
                    overlaying = "y"
                },
                yaxis = new Yaxis()
                {
                    title = "count",
                }
            }, Chart.Plot(new[] {vusTrace, rpsTrace, fitTrace}));
            Chart.Show(chart);
            var startTimeStr = TimeHelper.ToRFC3339Time(startTime);
            var endTimeStr = TimeHelper.ToRFC3339Time(endTime);
            Console.WriteLine($"start time: {startTimeStr}");
            Console.WriteLine($"end time: {endTimeStr}");
            var cpuTrace = await GetCpuTrace(startTimeStr, endTimeStr);
            var chart2 = Chart.WithLayout(new Layout.Layout()
            {
                yaxis = new Yaxis()
                {
                    title = "count"
                },
                yaxis2 = new Yaxis()
                {
                    title = "%",
                    side = "right",
                    overlaying = "y"
                }
            }, Chart.Plot(new[] {vusTrace, cpuTrace}));
            Chart.Show(chart2);
            
            
            var readTrace = await GetReadTrace(startTimeStr, endTimeStr);
            var chart3 = Chart.WithLayout(new Layout.Layout()
            {
                yaxis = new Yaxis()
                {
                    title = "count"
                },
                yaxis2 = new Yaxis()
                {
                    title = "count/s",
                    side = "right",
                    overlaying = "y"
                }
            }, Chart.Plot(new[] {vusTrace, readTrace}));
            Chart.Show(chart3);
            
            var lockTrace = await GetSqlLockTrace(startTimeStr, endTimeStr);
            var chart4 = Chart.WithLayout(new Layout.Layout()
            {
                yaxis = new Yaxis()
                {
                    title = "count"
                },
                yaxis2 = new Yaxis()
                {
                    title = "count/s",
                    side = "right",
                    overlaying = "y"
                }
            }, Chart.Plot(new[] {vusTrace, lockTrace}));
            Chart.Show(chart4);
        }

        private static Scatter GetTraceFromJson(string[] args, out Scatter fitTrace, out Scatter vusTrace,
            out DateTime startTime, out DateTime endTime)
        {
            var filePath = "";
            if (args.Length == 0)
            {
                var currentDic = Directory.GetCurrentDirectory();
                filePath = Directory.GetFiles(currentDic, "*.json").OrderByDescending(File.GetLastWriteTime).First();
            }
            else
            {
                filePath = args[0];
            }

            var lines = File.ReadLines(filePath).AsParallel().WithDegreeOfParallelism(Environment.ProcessorCount)
                .Select(JsonObject.Parse).ToList();
            var pointsList = lines.Where(it =>
                it["type"] == "Point"
            ).ToList();
            var gracefulTimespan = TimeSpan.FromSeconds(30);
            startTime = pointsList.Min(it => ParseShortestXsdDateTime(it.Object("data")["time"]))
                .Floor(TimeSpan.FromSeconds(1));
            endTime = pointsList.Max(it => ParseShortestXsdDateTime(it.Object("data")["time"]))
                .Subtract(gracefulTimespan);
            var rpsList = new ConcurrentDictionary<int, int>(Enumerable
                .Range(0, (int) Math.Ceiling((endTime - startTime).TotalSeconds) + 1)
                .Select(it => it).ToDictionary(it => it, it => 0));
            var httpReqDurationList = pointsList.Where(it => it["metric"] == "http_req_duration")
                .ToList();
            var tempStartTime = startTime;
            Parallel.ForEach(httpReqDurationList, httpReqDuration =>
            {
                var startIndex =
                    (ParseShortestXsdDateTime(httpReqDuration.Object("data")["time"])
                        .Floor(TimeSpan.FromSeconds(1)) - tempStartTime).TotalSeconds;
                var duration = TimeSpan
                    .FromMilliseconds(double.Parse(httpReqDuration.Object("data")["value"])).TotalSeconds;
                var endIndex = (int) Math.Floor(startIndex + duration);
                if (!rpsList.ContainsKey(endIndex))
                {
                    return;
                }

                rpsList[endIndex] = rpsList[endIndex] + 1;
            });
            var rpsTraceDic = rpsList.GroupBy(it => it.Key / 60).ToDictionary(it => it.Key, it => it.Average(
                it => it.Value));
            if (rpsTraceDic[rpsTraceDic.Count - 1] == 0)
            {
                rpsTraceDic.Remove(rpsTraceDic.Count - 1);
            }

            var x = rpsTraceDic.Select(it => tempStartTime.AddMinutes(it.Key)).ToList();
            var rpsTrace = new Scatter()
            {
                x = x,
                y = rpsTraceDic.Values.ToList(),
                name = "Throughput(request/s)",
                yaxis = "y2"
            };

            var limitx = -1;
            for (var i = 2; i < rpsTraceDic.Count - 1; i++)
            {
                var (_, tempk1) = Fit.Line(rpsTraceDic.Keys.Take(i).Select(x => (double) x).ToArray(),
                    rpsTraceDic.Values.Take(i).ToArray());
                var (_, tempk2) = Fit.Line(rpsTraceDic.Keys.Take(i + 1).Select(x => (double) x).ToArray(),
                    rpsTraceDic.Values.Take(i + 1).ToArray());
                if (!((tempk1 - tempk2) > 0.1))
                {
                    continue;
                }

                limitx = i;
                break;
            }

            var (b, k) = Fit.Line(rpsTraceDic.Keys.Take(limitx).Select(x => (double) x).ToArray(),
                rpsTraceDic.Values.Take(limitx).ToArray());

            fitTrace = new Scatter()
            {
                x = x,
                y = rpsTraceDic.Keys.Select(i => k * i + b).ToList(),
                name = "Expected throughput(request/s)",
                yaxis = "y2"
            };

            var vusData = lines.Where(it =>
                    it["type"] == "Point" &&
                    it["metric"] == "vus").Select(it =>
                    new Tuple<DateTime, int>(ParseShortestXsdDateTime(it.Object("data")["time"]),
                        int.Parse(it.Object("data")["value"])))
                .OrderBy(it => it.Item1).GroupBy(it => it.Item1.Floor(TimeSpan.FromMinutes(1)))
                .Select(it => new Tuple<DateTime, int>(it.Key.Floor(TimeSpan.FromMinutes(1)), it.Max(t => t.Item2)))
                .OrderBy(it => it.Item1)
                .ToList();
            var vusMaxTime = vusData.Max(it => it.Item1).Subtract(gracefulTimespan);
            vusData = vusData.Where(it => it.Item1 < vusMaxTime).ToList();
            vusTrace = new Scatter()
            {
                x = vusData.Select(it => it.Item1.AddSeconds(tempStartTime.Second)),
                y = vusData.Select(it => it.Item2),
                name = "virtual user count",
            };
            var checksData = lines.Where(it => it["type"] == "Point" &&
                                               it["metric"] == "checks").OrderBy(it =>
                ParseShortestXsdDateTime(it.Object("data")["time"])).ToList();

            var checkTrace = new Scatter()
            {
                x = checksData.Select(it => ParseShortestXsdDateTime(it.Object("data")["time"])),
                y = checksData.Select(it => int.Parse(it.Object("data")["value"]) * 100),
                name = "check",
                yaxis = "y2"
            };
            return rpsTrace;
        }

        private static async Task<Scatter> GetCpuTrace(string startTime, string endTime)
        {
            var influxDBClient = InfluxDbHelper.Client.Value;
            var vusPoints = await influxDBClient.GetPointFromInfluxDb(startTime, endTime,
                @"
  |> filter(fn: (r) => r._measurement == ""cpu"")
  |> filter(fn: (r) => r._field == ""usage_user"" or r._field == ""usage_system"" )
  |> filter(fn: (r) => r.cpu == ""cpu-total"")
  |> filter(fn: (r) => r[""host""] ==  ""app-Server"")
  |> pivot(rowKey:[""_time""], columnKey: [""_field""], valueColumn: ""_value"")
  |> map(fn: (r) => ({ r with _value: r.usage_user + r.usage_system }))
");
            var cpuTrace = new Scatter()
            {
                x = vusPoints.Select(it => it.Time.AddHours(8)),
                y = vusPoints.Select(it => it.Data1),
                name = "app server cpu(percent)",
                yaxis = "y2"
            };
            return cpuTrace;
        }
        
        private static async Task<Scatter> GetReadTrace(string startTime, string endTime)
        {
            var influxDBClient = InfluxDbHelper.Client.Value;
            var vusPoints = await influxDBClient.GetPointFromInfluxDb(startTime, endTime,
                @"
                      |> filter(fn: (r) => r[""_measurement""] == ""procstat"")
                      |> filter(fn: (r) => r[""_field""] == ""read_count"" or r[""_field""] == ""write_count"")
                      |> filter(fn: (r) => r[""exe""] == ""ForguncyServerConsole.exe"")
                      |> filter(fn: (r) => r[""process_name""] == ""ForguncyServerConsole.exe"")
                      |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)
                      |> derivative(unit: 1s, nonNegative: false)
                      |> yield(name: ""derivative"")
");
            var cpuTrace = new Scatter()
            {
                x = vusPoints.Select(it => it.Time.AddHours(8)),
                y = vusPoints.Select(it => it.Data1),
                name = "app server read(count/s)",
                yaxis = "y2"
            };
            return cpuTrace;
        }
        
        
        private static async Task<Scatter> GetSqlLockTrace(string startTime, string endTime)
        {
            var influxDBClient = InfluxDbHelper.Client.Value;
            var vusPoints = await influxDBClient.GetPointFromInfluxDb(startTime, endTime,
                @"
                      |> filter(fn: (r) => r[""_field""] == ""value"")
                      |> filter(fn: (r) => r[""counter""] == ""Lock Waits/sec"")
                      |> derivative(unit: 1s, nonNegative: false)
                      |> yield(name: ""derivative"")
",null,"mssql");
            var cpuTrace = new Scatter()
            {
                x = vusPoints.Select(it => it.Time.AddHours(8)),
                y = vusPoints.Select(it => it.Data1),
                name = "app server read(count/s)",
                yaxis = "y2"
            };
            return cpuTrace;
        }
    }
    
    
    

    public class Point
    {
        public DateTime Time { get; set; }
        public float Data1 { get; set; }
        public float Data2 { get; set; }
        public float Data3 { get; set; }
        public float Data4 { get; set; }
    }

    public static class InfluxDbHelper
    {
        public static Lazy<InfluxDBClient> Client = new Lazy<InfluxDBClient>(() =>
            InfluxDBClientFactory.Create(_url.Value,
                Environment.GetEnvironmentVariable("influxDBv2Token")));

        public static Lazy<string> Bucket = new Lazy<string>(() =>
        {
            return Environment.GetEnvironmentVariable("influxDBv2Bucket");
        });

        public static Lazy<string> Organization = new Lazy<string>(() =>
        {
            return Environment.GetEnvironmentVariable("influxDBv2Organization");
        });

        private static Lazy<string> _url = new Lazy<string>(() =>
        {
            return Environment.GetEnvironmentVariable("influxDBv2Url");
        });

        public static async Task<List<Point>> GetPointFromInfluxDb(this InfluxDBClient influxDBClient, string startTime,
            string endTime, string query, Func<FluxRecord, Point> mapFunc = null,string bucket = null)
        {
            mapFunc ??= fluxRecord => new Point()
            {
                Time = fluxRecord.GetTime().Value.ToDateTimeUtc(),
                Data1 = float.Parse(fluxRecord.GetValue().ToString())
            };
            var actualEndTime =
                TimeHelper.ToRFC3339Time(TimeHelper.FromRFC3339Time(endTime).Floor(TimeSpan.FromMinutes(1)));
            var fluxQuery = $@"from(bucket: ""{bucket ?? Bucket.Value}"")
                              |> range(start: {startTime},stop: {actualEndTime})
                              {query} 
                              ";
            var fluxTables =
                await influxDBClient.GetQueryApi().QueryAsync(fluxQuery, Organization.Value);
            var vusPoints = fluxTables.First().Records.Select(mapFunc).ToList();
            return vusPoints;
        }
    }

    public static class TimeHelper
    {
        public static string ToRFC3339Time(DateTime source)
        {
            return XmlConvert.ToString(source, XmlDateTimeSerializationMode.Utc);
        }

        public static DateTime FromRFC3339Time(string source)
        {
            return XmlConvert.ToDateTime(source);
        }
    }

    public static class DateTimeExtensions
    {
        public static DateTime Floor(this DateTime dateTime, TimeSpan interval)
        {
            return dateTime.AddTicks(-(dateTime.Ticks % interval.Ticks));
        }

        public static DateTime Ceiling(this DateTime dateTime, TimeSpan interval)
        {
            var overflow = dateTime.Ticks % interval.Ticks;

            return overflow == 0 ? dateTime : dateTime.AddTicks(interval.Ticks - overflow);
        }

        public static DateTime Round(this DateTime dateTime, TimeSpan interval)
        {
            var halfIntervalTicks = (interval.Ticks + 1) >> 1;

            return dateTime.AddTicks(halfIntervalTicks - ((dateTime.Ticks + halfIntervalTicks) % interval.Ticks));
        }
    }
}