//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Codeajans">
//     Copyright (c) Fatih Tatoğlu, Codeajans. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Codeajans.IIS2SQL
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Data.SqlClient;
    using System.IO;

    using global::IIS2SQL.Properties;

    /// <summary>
    /// Contains the program entry point.
    /// </summary>
    public class Program
    {
        /// <summary>
        /// The raw log database table name.
        /// </summary>
        private const string TableName = "RawLog";

        /// <summary>
        /// Defines the program entry point. 
        /// </summary>
        /// <param name="args">An array of <see cref="System.String"/> containing command line parameters.</param>
        private static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("------------------------------------------------------------");
                Console.WriteLine("IIS Log to SQL Server by [Fatih Tatoğlu]");
                Console.WriteLine("The required database table will be created.");
                Console.WriteLine("The connection string 'LogDB' must be in application configuration file.");
                Console.WriteLine();
                Console.WriteLine("Usage: iis2sql [logs_folder_path]");
                Console.WriteLine("For current folder, use '.' ('.\\logs\\')");
                Console.WriteLine("------------------------------------------------------------");
                Console.WriteLine();

                return;
            }

            SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings["LogDB"].ConnectionString);

            bool isTableExist = IsTableExist(Program.TableName, connection);
            if (!isTableExist)
            {
                string query = string.Format(Resources.RawLog_CreateTable, Program.TableName);
                ExecuteNonQuery(query, null, connection);
            }

            string path = args[0];
            if (path.StartsWith("."))
            {
                path = Path.Combine(Directory.GetCurrentDirectory(), path.Replace(".\\", string.Empty));
            }

            string[] files = Directory.GetFiles(path, "*.log");
            Console.WriteLine("{0} file(s) ready for transfer.", files.Length);
            Console.WriteLine();
            for (int i = 0; i < files.Length; i++)
            {
                string filePath = Path.Combine(path, files[i]);
                if (!File.Exists(filePath))
                {
                    continue;
                }

                if (!IsInternetInformationServiceLog(filePath))
                {
                    Console.WriteLine("[{0}]\t{1} is not an IIS log file.", i + 1, files[i]);
                    continue;
                }

                DateTime startDate = DateTime.UtcNow;

                ProcessLogFile(connection, filePath);

                var diff = DateTime.UtcNow - startDate;
                Console.WriteLine("[{2}]\t{0} transferred. {1} min", files[i], diff.TotalMinutes, i + 1);
            }

            Console.WriteLine("Transfer finished.");
        }

        /// <summary>
        /// Determines raw log table is exist.
        /// </summary>
        /// <param name="tableName">The table name.</param>
        /// <param name="connection">The database connection.</param>
        /// <returns><c>true</c> or <c>false</c>.</returns>
        private static bool IsTableExist(string tableName, SqlConnection connection)
        {
            bool result = false;

            string query = "SELECT CAST(COUNT(*) AS bit) FROM INFORMATION_SCHEMA.TABLES (NOLOCK) WHERE TABLE_NAME = @TableName";
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@TableName", tableName);

                try
                {
                    if (connection.State != System.Data.ConnectionState.Open)
                    {
                        connection.Open();
                    }

                    object rawData = command.ExecuteScalar();
                    bool.TryParse(rawData.ToString(), out result);
                }
                finally
                {
                    if (connection.State != System.Data.ConnectionState.Closed)
                    {
                        connection.Close();
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Executes query.
        /// </summary>
        /// <param name="query">The query statement.</param>
        /// <param name="parameters">The query parameters.</param>
        /// <param name="connection">The database connection.</param>
        private static void ExecuteNonQuery(string query, Dictionary<string, object> parameters, SqlConnection connection)
        {
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                if (parameters != null)
                {
                    foreach (var item in parameters)
                    {
                        command.Parameters.AddWithValue(item.Key, item.Value);
                    }
                }

                try
                {
                    if (connection.State != System.Data.ConnectionState.Open)
                    {
                        connection.Open();
                    }

                    command.ExecuteNonQuery();
                }
                finally
                {
                    if (connection.State != System.Data.ConnectionState.Closed)
                    {
                        connection.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Determines log file is an internet information service log.
        /// </summary>
        /// <param name="filePath">The path of the log file.</param>
        /// <returns><c>true</c> or <c>false</c>.</returns>
        private static bool IsInternetInformationServiceLog(string filePath)
        {
            bool result = false;
            using (StreamReader reader = new StreamReader(filePath))
            {
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    if (line.StartsWith("#Software: Microsoft Internet Information Services"))
                    {
                        result = true;
                        break;
                    }
                }

                reader.Close();
            }

            return result;
        }

        /// <summary>
        /// Processes log file.
        /// </summary>
        /// <param name="connection">The database connection.</param>
        /// <param name="filePath">The path of the log file.</param>
        private static void ProcessLogFile(SqlConnection connection, string filePath)
        {
            string fields = string.Empty;
            using (StreamReader reader = new StreamReader(filePath))
            {
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    if (line.StartsWith("#Fields"))
                    {
                        fields = line.Replace("#Fields: ", string.Empty);
                        fields = fields.Replace(" ", "],[");
                    }

                    if (line.StartsWith("#"))
                    {
                        continue;
                    }

                    line = line
                        .Replace("'", "''")
                        .Replace(" ", "','")
                        .Replace("'-'", "NULL");

                    string query = "INSERT INTO [{0}]([" + fields + "]) VALUES ('" + line + "')";
                    query = string.Format(query, Program.TableName);

                    ExecuteNonQuery(query, null, connection);
                }

                reader.Close();
            }
        }
    }
}