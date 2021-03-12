using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MathNet.Numerics;
using ServiceStack.Text;
using XPlot.Plotly;
using static ServiceStack.Text.Common.DateTimeSerializer;

namespace K6RpsSolution
{
    class Program
    {
        static void Main(string[] args)
        {
            var currentDic = Directory.GetCurrentDirectory();
            var filePath = Directory.GetFiles(currentDic, "*.json").OrderByDescending(File.GetLastWriteTime).First();
            var lines = File.ReadLines(filePath).AsParallel().WithDegreeOfParallelism(Environment.ProcessorCount)
                .Select(JsonObject.Parse).ToList();
            var pointsList = lines.Where(it =>
                it["type"] == "Point"
            ).ToList();
            var startTime = pointsList.Min(it => ParseShortestXsdDateTime(it.Object("data")["time"]))
                .Floor(TimeSpan.FromSeconds(1));
            var endTime = pointsList.Max(it => ParseShortestXsdDateTime(it.Object("data")["time"]));
            var rpsList = new ConcurrentDictionary<int, int>(Enumerable
                .Range(0, (int) Math.Ceiling((endTime - startTime).TotalSeconds) + 1)
                .Select(it => it).ToDictionary(it => it, it => 0));
            var httpReqDurationList = pointsList.Where(it => it["metric"] == "http_req_duration")
                .ToList();
            Parallel.ForEach(httpReqDurationList, httpReqDuration =>
            {
                var startIndex =
                    (ParseShortestXsdDateTime(httpReqDuration.Object("data")["time"])
                        .Floor(TimeSpan.FromSeconds(1)) - startTime).TotalSeconds;
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

            var x = rpsTraceDic.Select(it => startTime.AddMinutes(it.Key + 1)).ToList();
            var rpsTrace = new Scatter()
            {
                x = x,
                y = rpsTraceDic.Values.ToList(),
                name = "Throughput(request/s)",
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

            var fitTrace = new Scatter()
            {
                x = x,
                y = rpsTraceDic.Keys.Select(x => k * x + b).ToList(),
                name = "Expected throughput(request/s)"
            };

            var vusData = lines.Where(it =>
                    it["type"] == "Point" &&
                    it["metric"] == "vus").ToList()
                .OrderBy(it => ParseShortestXsdDateTime(it.Object("data")["time"])).ToList();
            var vusTrace = new Scatter()
            {
                x = vusData.Select(it => ParseShortestXsdDateTime(it.Object("data")["time"])),
                y = vusData.Select(it => int.Parse(it.Object("data")["value"])),
                name = "virtual user count",
            };
            var checksData = lines.Where(it => it["type"] == "Point" &&
                                               it["metric"] == "checks").OrderBy(it =>
                ParseShortestXsdDateTime(it.Object("data")["time"])).ToList();
            ;
            var checkTrace = new Scatter()
            {
                x = checksData.Select(it => ParseShortestXsdDateTime(it.Object("data")["time"])),
                y = checksData.Select(it => int.Parse(it.Object("data")["value"]) * 100),
                name = "check",
            };
            var chart = Chart.Plot(new[] {vusTrace, rpsTrace, fitTrace});
            Chart.Show(chart);
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