using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace EventStore.FluentClient.Tests.Utils
{
    class EsProcess
    {

        private static Process _esProcess;
        private static int _callers;
        private static string _ip;
        private static string _httpPort;
        /// <summary>
        /// Attempts to Start in memory Event Store unless already running
        /// </summary>
        /// <returns>Indicates whether new instance of Event Store was started.</returns>
        public static async Task<bool> RequestStart(string settingsKey, string exeKey)
        {
            _callers++;
            if (_esProcess != null && !_esProcess.HasExited)
                return false;
            string esArgs = ConfigurationManager.ConnectionStrings[settingsKey].ConnectionString;



            var settings = esArgs.Trim().Split(' ').ToDictionary(setting => setting.Split('=')[0].Trim(), setting => setting.Split('=').Length > 1 ? setting.Split('=')[1].Trim() : String.Empty);
            _ip = settings["--int-ip"];
            _httpPort = settings["--int-http-port"];

            var installInfo = new ProcessStartInfo
            {
                FileName = GetEsExecutablePath(exeKey),
                Arguments = esArgs,
            };
            _esProcess = Process.Start(installInfo);

            await UntilEventStoreRunning(TimeSpan.FromSeconds(60));

            return _callers == 1;
        }

        private static async Task UntilEventStoreRunning(TimeSpan timeout)
        {
            var client = new HttpClient();
            var sw = new Stopwatch();
            sw.Start();
            while (sw.Elapsed < timeout)
            {
                try
                {
                    var response = await client.GetAsync(String.Format("http://{0}:{1}/projection/$users/state", _ip, _httpPort));
                    if (response.StatusCode == HttpStatusCode.OK)
                        return;

                }
                // ReSharper disable once EmptyGeneralCatchClause
                catch
                {
                }
                Thread.Sleep(500);
            }
            sw.Stop();
            ForceStop();
            throw new ApplicationException("Failed to Start Event Store in timely manner.");

        }

        /// <summary>
        /// Attempts to Stop in memory Event Store unless other tasks depend on it
        /// </summary>
        /// <returns>Indicates whether Event Store process was killed</returns>
        public static bool RequestStop()
        {
            if (_esProcess.HasExited)
            {
                _callers = 0;
                return true;
            }
            if (_callers == 0 || --_callers == 0)
            {
                _esProcess.Kill();
                return true;
            }
            return false;
        }

        /// <summary>
        /// Forces Event Store process to stop ignoring other tasks that might depend on it
        /// </summary>
        public static void ForceStop()
        {
            _callers = 0;
            if (_esProcess != null && !_esProcess.HasExited)
            {
                _esProcess.Kill();
                _esProcess.WaitForExit();
            }
        }


        private static string GetEsExecutablePath(string exePath)
        {
            var assembly = System.Reflection.Assembly.GetExecutingAssembly().CodeBase;
            var basePath = new FileInfo(new Uri(assembly).LocalPath).DirectoryName;

            
            // ReSharper disable once AssignNullToNotNullAttribute
            string esExecutable = Path.GetFullPath(Path.Combine(basePath, ConfigurationManager.AppSettings[exePath]));
            return esExecutable;
            
        }
    }
}
