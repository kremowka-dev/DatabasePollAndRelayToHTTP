//Kremowka End User Licence and Service Agreement. Version 1.0 (EULSA v1.0).
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Reflection;
using System.Threading;
using DatabasePollAndRelayToHTTP.DynamicEnvironment;
using DatabasePollAndRelayToHTTP.Models;
using log4net;
using log4net.Config;

namespace DatabasePollAndRelayToHTTP
{
    internal static class Program
    {
        private static ILog _log;
        private static readonly List<SourceTableDefinition> SourceTableDefinitions = new();
        private static readonly List<Thread> SourceTableDefinitionThreads = new();
        private static DynamicEnvironmentVariables _environment;

        private static void Main()
        {
            _log = StartLogger();
            _environment = new DynamicEnvironmentVariables(_log);

            _log.Debug(
                $"Database Poll and Relay Main: Has established the environment variables.  Will proceed to create a connection to {_environment.AppSettings("ConnectionStringTarget")}.");

            var sqlConSource =
                DatabaseHelpers.OpenDatabaseConnection(_environment.AppSettings("ConnectionStringTarget"), _log);

            _log.Info(
                "Database Poll and Relay Main: Will proceed to create test the validity of the source connection.");

            if (sqlConSource != null)
            {
                if (sqlConSource.State == ConnectionState.Open)
                {
                    _log.Info(
                        "Database Poll and Relay Main: Connection is instantiated and open.  Will now proceed to get the source table definitions.");

                    GetSourceTableDefinitions(sqlConSource);

                    _log.Info(
                        "Database Poll and Relay Main: Has built the source table definitions.  Will now proceed to start the threads for each source table definition.");

                    StartThreads();

                    _log.Info(
                        "Database Poll and Relay Main: Has started all of the threads for the source table definitions.  Will block the closure of the application with a Console.ReadKey.  Press any key to terminate.");

                    Console.ReadKey();

                    _log.Info("Database Poll and Relay Main: Terminating.  Will stop threads.");

                    foreach (var sourceTableDefinition in SourceTableDefinitions)
                    {
                        sourceTableDefinition.Stopping = true;
                        _log.Info(
                            $"Database Poll and Relay Main: Terminating.  Signalled stop to Source Table Definition ID {sourceTableDefinition.SourceTableDefinitionId}.");
                    }

                    _log.Info(
                        "Database Poll and Relay Main: Terminating.  Has signalled to stop in an elegant fashion.");

                    do
                    {
                        _log.Info(
                            $"Database Poll and Relay Main: Terminating.  Active threads {SourceTableDefinitionThreads.FindAll(x => x.IsAlive).Count > 0}.");
                        Thread.Sleep(1000);
                    } while (SourceTableDefinitionThreads.FindAll(x => x.IsAlive).Count > 0);

                    _log.Info("Database Poll and Relay Main: Terminating.  Elegant termination.");
                }
                else
                {
                    _log.Debug(
                        $"Database Poll and Relay Main: Connection is not in an open state,  instead {sqlConSource.State}.");
                }
            }
            else
            {
                _log.Error(
                    "Database Poll and Relay Main: Could not make a database connection to the source. Application will now terminate.");
            }
        }

        private static void StartThreads()
        {
            foreach (var sourceTableDefinition in SourceTableDefinitions)
            {
                _log.Info("Database Poll and Relay Start Threads: Is about to start a new thread for " +
                          sourceTableDefinition.CommandText + ".");
                var processingSourceTableDefinition = new ProcessingSourceTableDefinition
                {
                    Log = _log, Environment = _environment, SourceTableDefinition = sourceTableDefinition
                };

                ThreadStart startSourceTableDefinitionThread =
                    processingSourceTableDefinition.Start;
                var sourceTableDefinitionThread = new Thread(startSourceTableDefinitionThread)
                {
                    IsBackground = true, Priority = ThreadPriority.Normal
                };
                sourceTableDefinitionThread.Start();
                SourceTableDefinitionThreads.Add(sourceTableDefinitionThread);
                _log.Info("Database Poll and Relay Start Threads: Has Started a new thread for " +
                          sourceTableDefinition.CommandText + ".");
            }
        }

        private static void GetSourceTableDefinitions(SqlConnection sqlConSource)
        {
            var sqlSourceTableDefinitions = new SqlCommand
            {
                CommandText = "Get_Source_Table_Definitions",
                CommandTimeout = Convert.ToInt32(_environment.AppSettings("CommandTimeout")),
                CommandType = CommandType.StoredProcedure,
                Connection = sqlConSource
            };

            _log.Info("Database Poll and Relay Start Threads: Is about to run the reader to return all table targets.");

            var sqlReaderTableDefinitions = sqlSourceTableDefinitions.ExecuteReader();

            _log.Info(
                "Database Poll and Relay Start Threads: Has executed a reader to return all Sources for the Sanctions Cache.");
            while (sqlReaderTableDefinitions.Read())
            {
                var insertColumnSourceTableDefinition = new SourceTableDefinition();
                try
                {
                    insertColumnSourceTableDefinition.SourceTableDefinitionId =
                        Convert.ToInt32(sqlReaderTableDefinitions["Source_Table_Definition_ID"]);
                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Name"]))
                    {
                        insertColumnSourceTableDefinition.Name = Convert.ToString(sqlReaderTableDefinitions["Name"]);
                        _log.Info("Database Poll and Relay Start Threads: Has found a name of " +
                                  sqlReaderTableDefinitions["Name"] +
                                  " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.Name = "";
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a name for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Command_Text"]))
                    {
                        insertColumnSourceTableDefinition.CommandText =
                            sqlReaderTableDefinitions["Command_Text"].ToString();
                        _log.Info("Database Poll and Relay Start Threads: Has found a Command Text of " +
                                  sqlReaderTableDefinitions["Command_Text"] + " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.CommandText = "";
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a Command Text for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Endpoint"]))
                    {
                        insertColumnSourceTableDefinition.Endpoint = sqlReaderTableDefinitions["Endpoint"].ToString();
                        _log.Info("Database Poll and Relay Start Threads: Has found a Endpoint of " +
                                  sqlReaderTableDefinitions["Endpoint"] + " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.Endpoint = "";
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a Endpoint for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Command_Type"]))
                    {
                        insertColumnSourceTableDefinition.CommandType =
                            (byte) sqlReaderTableDefinitions["Command_Type"];
                        _log.Info("Database Poll and Relay Start Threads: Has found a Command Type of " +
                                  sqlReaderTableDefinitions["Command_Text"] + " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.CommandType = 1;
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a Command Type for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Limit"]))
                    {
                        insertColumnSourceTableDefinition.Limit = (int) sqlReaderTableDefinitions["Limit"];
                        _log.Info("Database Poll and Relay Start Threads: Has found a Limit of " +
                                  sqlReaderTableDefinitions["Limit"] +
                                  " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.Limit = 1000;
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a Limit for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Row_Version_Column_Name"]))
                    {
                        insertColumnSourceTableDefinition.RowVersionColumnName =
                            sqlReaderTableDefinitions["Row_Version_Column_Name"].ToString();
                        _log.Info("Database Poll and Relay Start Threads: Has found a Row Version Column Name of " +
                                  sqlReaderTableDefinitions["Row_Version_Column_Name"] +
                                  " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.RowVersionColumnName = "";
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a Row Version Column Name for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    if (!Convert.IsDBNull(sqlReaderTableDefinitions["Table_Poll_Interval"]))
                    {
                        insertColumnSourceTableDefinition.TablePollInterval =
                            (int) sqlReaderTableDefinitions["Table_Poll_Interval"];
                        _log.Info("Database Poll and Relay Start Threads: Has found a Table Poll Interval of " +
                                  sqlReaderTableDefinitions["Table_Poll_Interval"] + " for Table Definition id " +
                                  sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }
                    else
                    {
                        insertColumnSourceTableDefinition.TablePollInterval =
                            Convert.ToInt16(_environment.AppSettings("TablePollInterval"));
                        _log.Info(
                            "Database Poll and Relay Start Threads: Has not found a Table Poll Interval for Table Definition id " +
                            sqlReaderTableDefinitions["Source_Table_Definition_ID"] + ".");
                    }

                    SourceTableDefinitions.Add(insertColumnSourceTableDefinition);
                    _log.Info("Database Poll and Relay Start Threads: Has added the table name " +
                              insertColumnSourceTableDefinition.Name +
                              " to the collection.");
                }
                catch (Exception ex)
                {
                    _log.Error("Database Poll and Relay Start Threads: An error has been created as " + ex + ".");
                }
            }

            sqlReaderTableDefinitions.Close();
        }

        private static ILog StartLogger()
        {
            try
            {
                XmlConfigurator.Configure(new FileInfo(AppDomain.CurrentDomain.BaseDirectory +
                                                       "Logging.log4net"));
                var log = LogManager.GetLogger(MethodBase.GetCurrentMethod()?.DeclaringType);

                log.Info("Database Poll and Relay Start Logging: The logger has been created.  Returning ILog.");

                return log;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}