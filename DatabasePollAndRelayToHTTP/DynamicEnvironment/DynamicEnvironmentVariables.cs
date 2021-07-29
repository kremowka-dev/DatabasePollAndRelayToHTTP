//Kremowka End User Licence and Service Agreement. Version 1.0 (EULSA v1.0).
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using log4net;

namespace DatabasePollAndRelayToHTTP.DynamicEnvironment
{
    public class DynamicEnvironmentVariables
    {
        private readonly Dictionary<string, string> _appSettings;
        private readonly ILog _log;

        public DynamicEnvironmentVariables(ILog log)
        {
            try
            {
                _log = log;

                _log.Info(
                    "Database Poll and Relay Start Dynamic Environment Variables:  The default and hardcoded values are going to be added.");

                _appSettings = new Dictionary<string, string>
                {
                    {"TablePollInterval", "10000"},
                    {"CommandTimeout", "30000"},
                    {"EscapeJSONValue", "True"},
                    {"RetryMS", "1000"},
                    {"HTTPTimeout", "1000"},
                    {
                        "ConnectionStringTarget",
                        "Data Source=(local)\\SQLEXPRESS;Initial Catalog=Demo;Integrated Security=True;"
                    },
                    {
                        "ConnectionStringSource",
                        "Data Source=(local)\\SQLEXPRESS;Initial Catalog=Bank;Integrated Security=True;"
                    }
                };

                _log.Info(
                    "Database Poll and Relay Start Dynamic Environment Variables: The default and hardcoded values have been added,  will now debug write out.");

                foreach (var (key, value) in _appSettings)
                    _log.Debug(
                        $"Database Poll and Relay Start Dynamic Environment Variables: Default and hardcoded value Key: {key} Value:{value}.");

                var runningDirectory = Path.GetDirectoryName(Assembly.GetEntryAssembly()?.Location);
                var pathConfig = Path.Combine(runningDirectory ?? throw new InvalidOperationException(),
                    "DatabasePollAndRelayToHTTP.environment");
                var configFile = new FileInfo(pathConfig);
                var sr = new StreamReader(configFile.FullName);

                var line = sr.ReadLine();
                while (line != null)
                {
                    var lineSplits = line.Split('=', 2);
                    if (!lineSplits[0].StartsWith("#")) //'Commented, falling back to default.
                    {
                        if (_appSettings.ContainsKey(lineSplits[0]))
                        {
                            if (lineSplits[1] == _appSettings[lineSplits[0]])
                            {
                                log.Info(
                                    "Database Poll and Relay Start Dynamic Environment Variables: File configuration variable " +
                                    lineSplits[0] +
                                    " enjoys the same value in Application Settings so has not been overwritten.");
                            }
                            else
                            {
                                _appSettings[lineSplits[0]] = lineSplits[1];

                                log.Info(
                                    "Database Poll and Relay Start Dynamic Environment Variables: Environment variable " +
                                    lineSplits[0] + " has been updated as " + lineSplits[1] +
                                    " in Application Settings so has not been overwritten.");
                            }
                        }
                        else
                        {
                            log.Info(
                                "Database Poll and Relay Start Dynamic Environment Variables: File configuration variable " +
                                lineSplits[0] + " is not a valid Application Setting.");
                        }
                    }

                    line = sr.ReadLine();
                }

                _log.Info(
                    "Dynamic Variables:  The configuration file values have been added,  will now debug write out.");

                foreach (var (key, value) in _appSettings)
                    _log.Debug(
                        $"Database Poll and Relay Start Dynamic Environment Variables: Configuration file values value Key: {key} Value:{value}.");

                foreach (DictionaryEntry environmentVariable in Environment.GetEnvironmentVariables())
                    if (_appSettings.ContainsKey(Convert.ToString(environmentVariable.Key) ?? string.Empty))
                    {
                        if (Convert.ToString(environmentVariable.Value) ==
                            _appSettings[environmentVariable.Key.ToString() ?? string.Empty])
                        {
                            log.Info(
                                "Database Poll and Relay Start Dynamic Environment Variables: Environment variable " +
                                environmentVariable.Key +
                                " enjoys the same value in Application Settings so has not been overwritten.");
                        }
                        else
                        {
                            _appSettings[environmentVariable.Key.ToString() ?? string.Empty] =
                                Convert.ToString(environmentVariable.Value);

                            log.Info(
                                "Database Poll and Relay Start Dynamic Environment Variables: Environment variable " +
                                environmentVariable.Key + " has been updated as " + environmentVariable.Value +
                                " in Application Settings so has not been overwritten.");
                        }
                    }
                    else
                    {
                        log.Info("Database Poll and Relay Start Dynamic Environment Variables: Environment variable " +
                                 environmentVariable.Key + " is not a valid Application Setting.");
                    }

                _log.Info(
                    "Database Poll and Relay Start Dynamic Environment Variables: The environment variables have been added,  will now debug write out.");

                foreach (var (key, value) in _appSettings)
                    _log.Debug(
                        $"Database Poll and Relay Start Dynamic Environment Variables: Environment variables value Key: {key} Value:{value}.");
            }

            catch (Exception ex)
            {
                log.Error(
                    "Database Poll and Relay Start Dynamic Environment Variables: Error in starting Environment Synchronisation as " +
                    ex + ".");
            }
        }

        public string AppSettings(string key)
        {
            string value = null;
            try
            {
                if (_appSettings.ContainsKey(key)) value = _appSettings[key];
            }
            catch (Exception ex)
            {
                _log.Error(
                    "Database Poll and Relay Start Dynamic Environment Variables: Error fetching variable with key " +
                    key + " with error " + ex);
            }

            return value;
        }
    }
}