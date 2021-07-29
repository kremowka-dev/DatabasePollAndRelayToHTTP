//Kremowka End User Licence and Service Agreement. Version 1.0 (EULSA v1.0).
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Web;
using DatabasePollAndRelayToHTTP.DynamicEnvironment;
using log4net;
using Newtonsoft.Json.Linq;

namespace DatabasePollAndRelayToHTTP.Models
{
    public class ProcessingSourceTableDefinition
    {
        public ILog Log { get; init; }
        public DynamicEnvironmentVariables Environment { get; init; }
        public SourceTableDefinition SourceTableDefinition { get; init; }

        public void Start()
        {
            try
            {
                do
                {
                    var sqlConSource = new SqlConnection();
                    try
                    {
                        Log.Info(
                            "Database Poll and Relay Processing Table: is about to connect to the SQL Server database for Source.");
                        sqlConSource =
                            DatabaseHelpers.OpenDatabaseConnection(Environment.AppSettings("ConnectionStringSource"),
                                Log);
                        var sqlConTarget = DatabaseHelpers.OpenDatabaseConnection(
                            Environment.AppSettings("ConnectionStringTarget"),
                            Log);
                        Log.Info(
                            "Database Poll and Relay Processing Table: Will run the stored procedure to return all table targets for Source.");

                        var sqlNewData = new SqlCommand();
                        switch (SourceTableDefinition.CommandType)
                        {
                            case 1:
                                sqlNewData.CommandText = "select top " + SourceTableDefinition.Limit + " * from " +
                                                         SourceTableDefinition.CommandText + " where " +
                                                         SourceTableDefinition.RowVersionColumnName + " > " +
                                                         SourceTableDefinition.RowVersion + " order by " +
                                                         SourceTableDefinition.RowVersionColumnName + " asc";
                                sqlNewData.CommandType = CommandType.Text;
                                Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                         SourceTableDefinition.SourceTableDefinitionId +
                                         " has created SQL specification as" + sqlNewData.CommandText + ".");
                                break;
                            case 2:
                                sqlNewData.CommandText = SourceTableDefinition.CommandText;
                                sqlNewData.CommandType = CommandType.StoredProcedure;
                                sqlNewData.Parameters.AddWithValue("Limit", SourceTableDefinition.Limit);
                                Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                         SourceTableDefinition.SourceTableDefinitionId +
                                         " has created a stored procedure specification with last row version of " +
                                         SourceTableDefinition.RowVersion + " and Limit of " +
                                         SourceTableDefinition.Limit +
                                         ".");
                                break;
                            default:
                                sqlNewData.CommandText = SourceTableDefinition.CommandText;
                                sqlNewData.CommandType = CommandType.StoredProcedure;
                                break;
                        }

                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId + " Is about to run the SQL: " +
                                 sqlNewData.CommandText);

                        sqlNewData.CommandTimeout = Convert.ToInt32(Environment.AppSettings("CommandTimeout"));
                        sqlNewData.Connection = sqlConSource;

                        var sqlReaderNewData = sqlNewData.ExecuteReader();

                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId +
                                 " Has run the SQL and will now loop around the data forward only.");
                        var lastActionDateValue = default(DateTime);
                        var lastRowVersionValue = default(long);
                        while (sqlReaderNewData.Read())
                            try
                            {
                                var payload = new Dictionary<string, object>();
                                var ordinal = 0;
                                foreach (var columnName in Enumerable.Range(0, sqlReaderNewData.FieldCount)
                                    .Select(sqlReaderNewData.GetName).ToList())
                                    try
                                    {
                                        if (!Convert.IsDBNull(sqlReaderNewData[columnName]))
                                        {
                                            if (columnName == SourceTableDefinition.RowVersionColumnName)
                                            {
                                                lastRowVersionValue = Convert.ToInt64(sqlReaderNewData[columnName]);
                                                payload.Add(columnName, lastRowVersionValue);
                                                Log.Info(
                                                    "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                    SourceTableDefinition.SourceTableDefinitionId +
                                                    " Has added column " + columnName + " in table " +
                                                    SourceTableDefinition.CommandText + " with value " +
                                                    lastActionDateValue + " as Timestamp.");
                                            }
                                            else if (sqlReaderNewData.GetDataTypeName(ordinal).Contains("date"))
                                            {
                                                var dateValue = Convert.ToDateTime(sqlReaderNewData[columnName]);
                                                payload.Add(columnName, dateValue.ToString("u"));
                                                Log.Info(
                                                    "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                    SourceTableDefinition.SourceTableDefinitionId +
                                                    " Has added column " + columnName + " in table " +
                                                    SourceTableDefinition.CommandText + " with value " +
                                                    sqlReaderNewData[columnName] + " as Date.");
                                            }
                                            else if (sqlReaderNewData.GetDataTypeName(ordinal).Contains("binary"))
                                            {
                                                Log.Info(
                                                    "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                    SourceTableDefinition.SourceTableDefinitionId +
                                                    " Has added column " + columnName + " in table " +
                                                    SourceTableDefinition.CommandText + " with value " +
                                                    lastActionDateValue +
                                                    " as binary,  but we can't process that.");
                                            }
                                            else if (sqlReaderNewData.GetDataTypeName(ordinal).Contains("timestamp") &&
                                                     columnName != SourceTableDefinition.RowVersionColumnName)
                                            {
                                                Log.Info(
                                                    "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                    SourceTableDefinition.SourceTableDefinitionId +
                                                    " Has added column " + columnName + " in table " +
                                                    SourceTableDefinition.CommandText + " with value " +
                                                    lastActionDateValue +
                                                    " as timestamp,  but we can't process that.");
                                            }
                                            else if (sqlReaderNewData.GetDataTypeName(ordinal).Contains("image") &&
                                                     columnName != SourceTableDefinition.RowVersionColumnName)
                                            {
                                                Log.Info(
                                                    "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                    SourceTableDefinition.SourceTableDefinitionId +
                                                    " Has added column " + columnName + " in table " +
                                                    SourceTableDefinition.CommandText + " with value " +
                                                    lastActionDateValue +
                                                    " as image,  but we can't process that.");
                                            }
                                            else
                                            {
                                                payload.Add(columnName, sqlReaderNewData[columnName]);
                                                Log.Info(
                                                    "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                    SourceTableDefinition.SourceTableDefinitionId +
                                                    " Has added column " + columnName + " in table " +
                                                    SourceTableDefinition.CommandText + " with value " +
                                                    sqlReaderNewData[columnName] + " as String.");
                                            }
                                        }
                                        else
                                        {
                                            Log.Info(
                                                "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                                SourceTableDefinition.SourceTableDefinitionId +
                                                " Has found a null column " + columnName + " in table " +
                                                SourceTableDefinition.CommandText + ".");
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Info(
                                            "Database Poll and Relay Processing Table: Source Table Definition ID " +
                                            SourceTableDefinition.SourceTableDefinitionId +
                                            " has created an error as " + ex + " while processing column name " +
                                            columnName + ".");
                                    }
                                    finally
                                    {
                                        ordinal += 1;
                                    }

                                SendMessage(payload);
                                Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                         SourceTableDefinition.SourceTableDefinitionId +
                                         " Is about to send payload for table " + SourceTableDefinition.CommandText +
                                         ".");

                                if (lastRowVersionValue > SourceTableDefinition.RowVersion)
                                    SourceTableDefinition.RowVersion = lastRowVersionValue;
                                else
                                    Log.Info(
                                        "Database Poll and Relay Processing Table:  Out of sequence last row version of " +
                                        lastRowVersionValue + ".");

                                Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                         SourceTableDefinition.SourceTableDefinitionId + " Has sent payload for" +
                                         SourceTableDefinition.CommandText +
                                         ".  The last primary key value has been updated to " +
                                         SourceTableDefinition.RowVersion + " locally.");
                            }
                            catch (Exception ex)
                            {
                                Log.Error(
                                    "Database Poll and Relay Processing Table:  An error has been created inside the reader while as " +
                                    ex + ".");
                            }

                        sqlReaderNewData.Close();
                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId +
                                 " Has finished an cycle and will now set the last primary key value remotely.");

                        var sqlCmdSetSync = new SqlCommand
                        {
                            CommandText = "Update_Source_Table_Definition",
                            CommandTimeout = Convert.ToInt32(Environment.AppSettings("CommandTimeout"))
                        };
                        sqlCmdSetSync.Parameters.AddWithValue("Source_Table_Definition_ID",
                            SourceTableDefinition.SourceTableDefinitionId);
                        sqlCmdSetSync.Parameters.AddWithValue("Last_Row_Version_Value",
                            SourceTableDefinition.RowVersion);
                        sqlCmdSetSync.CommandType = CommandType.StoredProcedure;
                        sqlCmdSetSync.Connection = sqlConTarget;
                        sqlCmdSetSync.ExecuteNonQuery();

                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId +
                                 " Has finished an cycle and has set the last primary key value remotely to " +
                                 SourceTableDefinition.RowVersion + ".");
                    }
                    catch (Exception ex)
                    {
                        Log.Error("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                  SourceTableDefinition.SourceTableDefinitionId +
                                  " An error has been created inside the loop as " + ex + ".");
                    }
                    finally
                    {
                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId +
                                 "  is about to close database connections.");
                        sqlConSource.Close();
                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId + "  has closed database connections.");

                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId + "  Going to sleep.");
                        Thread.Sleep(SourceTableDefinition.TablePollInterval);
                        Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                                 SourceTableDefinition.SourceTableDefinitionId + "  Is awake.");
                    }
                } while (!SourceTableDefinition.Stopping);

                Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                         SourceTableDefinition.SourceTableDefinitionId + "  exiting on stop.");
            }
            catch (Exception ex)
            {
                Log.Error(
                    "Database Poll and Relay Processing Table:  An error has been created outside of the loop as " +
                    ex + ".");
            }
        }

        private void SendMessage(Dictionary<string, object> payload)
        {
            do
            {
                var retry = false;
                try
                {
                    var sb = new StringBuilder();
                    sb.Append('{');

                    var first = true;
                    foreach (var (key, value) in payload)
                    {
                        if (first)
                            first = false;
                        else
                            sb.Append(',');
                        sb.Append('"');
                        sb.Append(key);
                        sb.Append("\":\"");

                        if (Environment.AppSettings("EscapeJSONValue") == "True")
                            sb.Append(HttpUtility.JavaScriptStringEncode(value.ToString()));
                        else
                            sb.Append(value);

                        sb.Append('"');
                        sb.Append("\r\n");
                    }

                    sb.Append('}');

                    Log.Info("Database Poll and Relay Processing Table: Source Table Definition ID " +
                             SourceTableDefinition.SourceTableDefinitionId + " has created a POST body " + sb +
                             " will now try and send.");

                    var sw = new Stopwatch();
                    sw.Start();

                    try
                    {
                        var ms = new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
                        var bytes = ms.ToArray();

                        var url = SourceTableDefinition.Endpoint;

                        Log.Info("Database Poll and Relay Processing Table: Ready URL as " + url);

                        var request = WebRequest.Create(url);
                        request.Method = WebRequestMethods.Http.Post;
                        request.ContentLength = bytes.Length;
                        request.Timeout = int.Parse(Environment.AppSettings("HTTPTimeout"));

                        var writer = request.GetRequestStream();
                        writer.Write(bytes, 0, bytes.Length);
                        writer.Close();

                        var responseStream = request.GetResponse();
                        var reader = new StreamReader(responseStream.GetResponseStream() ??
                                                      throw new InvalidOperationException());
                        var response = JObject.Parse(reader.ReadToEnd());

                        var responseElevation = (int) response.SelectToken("$.ResponseElevation");

                        Log.Info("Database Poll and Relay Processing Table: Response is " + responseElevation);

                        if (responseElevation > 0)
                            Log.Info("BLOCKED"); //THIS IS WHERE WE WILL UPDATE THE SQL SERVER AT BANK FOR BLOCK STATUS.
                    }
                    catch (WebException ex)
                    {
                        if (ex.Response == null)
                        {
                            Log.Error("Send HTTP Message: Source Table Definition ID " +
                                      SourceTableDefinition.SourceTableDefinitionId + " has a response error of " +
                                      ex.Message + ".");
                        }
                        else
                        {
                            var responseString =
                                new StreamReader(ex.Response.GetResponseStream() ??
                                                 throw new InvalidOperationException()).ReadToEnd();
                            var responseStatus = ((HttpWebResponse) ex.Response).StatusCode;
                            if (responseString.Contains("Maximum Queue"))
                            {
                                Log.Error("Send HTTP Message: Source Table Definition ID " +
                                          SourceTableDefinition.SourceTableDefinitionId + " has a response status of " +
                                          responseStatus + " and a response message " + responseString +
                                          " has set retry.");
                                retry = true;
                            }
                            else
                            {
                                Log.Error("Send HTTP Message: Source Table Definition ID " +
                                          SourceTableDefinition.SourceTableDefinitionId + " has a response status of " +
                                          responseStatus + " and a response message in error of " + responseString);
                            }
                        }
                    }

                    sw.Stop();
                    Log.Info("Send HTTP Message: Source Table Definition ID " +
                             SourceTableDefinition.SourceTableDefinitionId + " has sent the POST body in " +
                             sw.ElapsedMilliseconds + "ms.");
                }
                catch (Exception ex)
                {
                    Log.Error("Send HTTP Message: Source Table Definition ID " +
                              SourceTableDefinition.SourceTableDefinitionId + " is in error " + ex);
                }

                if (!retry)
                    break;
                Thread.Sleep(Convert.ToInt16(Environment.AppSettings("RetryMS")));
            } while (!SourceTableDefinition.Stopping);
        }
    }
}