//Kremowka End User Licence and Service Agreement. Version 1.0 (EULSA v1.0).
using System;
using System.Data.SqlClient;
using log4net;

namespace DatabasePollAndRelayToHTTP.DynamicEnvironment
{
    public static class DatabaseHelpers
    {
        public static SqlConnection OpenDatabaseConnection(string connectionString, ILog log)
        {
            try
            {
                var sqlConSource = new SqlConnection();

                log.Info(
                    "Database Poll and Relay Start Connect To Database: is about to connect to the SQL Server database for the source database.");

                sqlConSource.ConnectionString = connectionString;
                sqlConSource.Open();

                log.Info(
                    "Database Poll and Relay Start Connect To Database: Has Opened a connection to the source database.");

                return sqlConSource;
            }
            catch (Exception e)
            {
                log.Error(
                    $"Database Poll and Relay Start Connect To Database: Could not open the database connection for exception {e}.");
                return null;
            }
        }
    }
}