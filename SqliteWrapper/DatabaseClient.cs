﻿using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Mono.Data.Sqlite;

namespace SqliteWrapper
{
    /// <summary>
    /// Lightweight wrapper for Sqlite.
    /// </summary>
    public class DatabaseClient : IDisposable
    {
        #region Public-Members

        public bool Debug;

        #endregion

        #region Private-Members

        private bool _Disposed = false;
        private string _Filename;
        private string _ConnectionString;
        private SqliteConnection _Connection;
         
        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initialize Sqlite client using a new file or existing file.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <param name="debug">Enable or disable console logging.</param>
        public DatabaseClient(string filename, bool debug)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));

            _Filename = filename;
            Debug = debug;

            BuildConnectionString();
            Connect();
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Tear down the database client and dispose of resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Use to sanitize values you wish to INSERT.
        /// </summary>
        /// <param name="s">Value to be sanitized.</param>
        /// <returns>Sanitized string.</returns>
        public static string SanitizeString(string s)
        {
            if (String.IsNullOrEmpty(s)) return String.Empty;

            string ret = "";
            int doubleDash = 0;
            int openComment = 0;
            int closeComment = 0;

            for (int i = 0; i < s.Length; i++)
            {
                if (((int)(s[i]) == 10) ||      // Preserve carriage return
                    ((int)(s[i]) == 13))        // and line feed
                {
                    ret += s[i];
                }
                else if ((int)(s[i]) < 32)
                {
                    continue;
                }
                else
                {
                    ret += s[i];
                }
            }

            //
            // double dash
            //
            doubleDash = 0;
            while (true)
            {
                doubleDash = ret.IndexOf("--");
                if (doubleDash < 0)
                {
                    break;
                }
                else
                {
                    ret = ret.Remove(doubleDash, 2);
                }
            }

            //
            // open comment
            // 
            openComment = 0;
            while (true)
            {
                openComment = ret.IndexOf("/*");
                if (openComment < 0) break;
                else
                {
                    ret = ret.Remove(openComment, 2);
                }
            }

            //
            // close comment
            //
            closeComment = 0;
            while (true)
            {
                closeComment = ret.IndexOf("*/");
                if (closeComment < 0) break;
                else
                {
                    ret = ret.Remove(closeComment, 2);
                }
            }

            //
            // in-string replacement
            //
            ret = ret.Replace("'", "''");

            return ret;
        }

        /// <summary>
        /// Create a string timestamp from the given DateTime for the database of the instance type.
        /// </summary>
        /// <param name="ts">DateTime.</param>
        /// <returns>A string with timestamp formatted for the database of the instance type.</returns>
        public string Timestamp(DateTime ts)
        {
            return ts.ToString("MM/dd/yyyy hh:mm:ss.fffffff tt");
        }

        /// <summary>
        /// Execute a SQL query.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="result">DataTable containing results.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public DataTable Query(string query)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(nameof(query));
            DataTable result = new DataTable();

            try
            {
                using (SqliteCommand cmd = new SqliteCommand(query, _Connection))
                {
                    using (SqliteDataReader rdr = cmd.ExecuteReader())
                    {
                        result.Load(rdr);
                        return result;
                    }
                }
            }
            finally
            {
                if (Debug)
                {
                    if (result != null)
                    {
                        Console.WriteLine(result.Rows.Count + " rows, query: " + query);
                    }
                }
            }
        }

        /// <summary>
        /// Execute a scalar SQL query (generally multiple statements in a single query).
        /// </summary>
        /// <param name="query">The query.</param>
        /// <param name="result">Object containing result from the query.</param>
        /// <returns>Boolean indicating success or failure.</returns>
        public object QueryScalar(string query)
        {
            object result = null;
            bool success = false;

            try
            {
                if (String.IsNullOrEmpty(query)) return false;

                using (SqliteCommand cmd = new SqliteCommand(query, _Connection))
                {
                    result = cmd.ExecuteScalar();
                    success = true;
                    return result;
                }
            }
            finally
            {
                if (Debug)
                {
                    if (result != null)
                    {
                        Console.WriteLine("Scalar success: " + success + ", result: " + result.ToString() + ", query: " + query);
                    }
                }
            }
        }

        /// <summary>
        /// Copies the database to another file.
        /// </summary>
        /// <param name="destination">The destination file.</param>
        public void Backup(string destination)
        {
            if (String.IsNullOrEmpty(destination)) throw new ArgumentNullException(nameof(destination));

            using (SqliteCommand cmd = new SqliteCommand("BEGIN IMMEDIATE;", _Connection))
            {
                cmd.ExecuteNonQuery();
            }
            
            File.Copy(_Filename, destination, true);
        
            using (SqliteCommand cmd = new SqliteCommand("ROLLBACK;", _Connection))
            {
                cmd.ExecuteNonQuery();
            }
        }
        
        /// <summary>
        /// Returns a DataTable containing at most one row with data from the specified table where the specified column contains the specified value.  Should only be used on key or unique fields.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="columnName">The column containing key or unique fields where a match is desired.</param>
        /// <param name="value">The value to match in the key or unique field column.  This should be an object that can be cast to a string value.</param>
        /// <returns>A DataTable containing at most one row.</returns>
        public DataTable GetUniqueObjectById(string tableName, string columnName, object value)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (String.IsNullOrEmpty(columnName)) throw new ArgumentNullException(nameof(columnName));
            if (value == null) throw new ArgumentNullException(nameof(value));

            Expression e = new Expression
            {
                LeftTerm = columnName,
                Operator = Operators.Equals,
                RightTerm = value.ToString()
            };

            return Select(tableName, null, 1, null, e, null);
        }

        /// <summary>
        /// Execute a SELECT query.
        /// </summary>
        /// <param name="tableName">The table from which you wish to SELECT.</param>
        /// <param name="indexStart">The starting index for retrieval; used for pagination in conjunction with maxResults and orderByClause.  orderByClause example: ORDER BY created DESC.</param>
        /// <param name="maxResults">The maximum number of results to retrieve.</param>
        /// <param name="returnFields">The fields you wish to have returned.  Null returns all.</param>
        /// <param name="filter">The expression containing the SELECT filter (i.e. WHERE clause data).</param>
        /// <param name="orderByClause">Specify an ORDER BY clause if desired.</param>
        /// <returns>A DataTable containing the results.</returns>
        public DataTable Select(string tableName, int? indexStart, int? maxResults, List<string> returnFields, Expression filter, string orderByClause)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            string outerQuery = "";
            string whereClause = "";
            DataTable result;

            //
            // SELECT
            //
            outerQuery += "SELECT ";

            //
            // fields
            //
            if (returnFields == null || returnFields.Count < 1) outerQuery += "* ";
            else
            {
                int fieldsAdded = 0;
                foreach (string curr in returnFields)
                {
                    if (fieldsAdded == 0)
                    {
                        outerQuery += SanitizeString(curr);
                        fieldsAdded++;
                    }
                    else
                    {
                        outerQuery += "," + SanitizeString(curr);
                        fieldsAdded++;
                    }
                }
            }
            outerQuery += " ";

            //
            // table
            //
            outerQuery += "FROM " + tableName + " ";

            //
            // expressions
            //
            if (filter != null)
            {
                whereClause = filter.ToWhereClause();
            }
            if (!String.IsNullOrEmpty(whereClause))
            {
                outerQuery += "WHERE " + whereClause + " ";
            }

            // 
            // order clause
            //
            if (!String.IsNullOrEmpty(orderByClause)) outerQuery += orderByClause + " ";

            //
            // limit
            //
            if (maxResults != null && maxResults > 0)
            {
                if (indexStart != null && indexStart >= 0)
                {
                    outerQuery += "LIMIT " + maxResults + " OFFSET " + indexStart;
                }
                else
                {
                    outerQuery += "LIMIT " + maxResults;
                }
            }
                     
            result = Query(outerQuery);
            return result;
        }

        /// <summary>
        /// Execute an INSERT query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to INSERT.</param>
        /// <param name="keyValuePairs">The key-value pairs for the row you wish to INSERT.</param>
        /// <returns>A DataTable containing the results.</returns>
        public object Insert(string tableName, Dictionary<string, object> keyValuePairs)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            string keys = "";
            string values = "";
            string query = "";

            #region Build-Key-Value-Pairs

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue;
                if (added == 0)
                {
                    keys += curr.Key;
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            values += "'" + Timestamp((DateTime)curr.Value) + "'";
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                values += "N'" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                            else
                            {
                                values += "'" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                        }
                    }
                    else values += "null";
                }
                else
                {
                    keys += "," + curr.Key;
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        { 
                            values += ",'" + Timestamp((DateTime)curr.Value) + "'";
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                values += ",N'" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                            else
                            {
                                values += ",'" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                        }

                    }
                    else values += ",null";
                }
                added++;
            }

            #endregion

            #region Build-INSERT-Query-and-Submit
             
            //
            // insert into
            //
            query += "INSERT INTO " + tableName + " ";
            query += "(" + keys + ") ";
            query += "VALUES ";
            query += "(" + values + "); ";
            query += "SELECT last_insert_rowid() AS id; ";
            
            return QueryScalar(query);

            #endregion
        }

        /// <summary>
        /// Execute an UPDATE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to UPDATE.</param>
        /// <param name="keyValuePairs">The key-value pairs for the data you wish to UPDATE.</param>
        /// <param name="filter">The expression containing the UPDATE filter (i.e. WHERE clause data).</param>
        public void Update(string tableName, Dictionary<string, object> keyValuePairs, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (keyValuePairs == null || keyValuePairs.Count < 1) throw new ArgumentNullException(nameof(keyValuePairs));

            string query = "";
            string keyValueClause = "";
            DataTable result;

            #region Build-Key-Value-Clause

            int added = 0;
            foreach (KeyValuePair<string, object> curr in keyValuePairs)
            {
                if (String.IsNullOrEmpty(curr.Key)) continue;
                if (added == 0)
                {
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        {
                            keyValueClause += curr.Key + "='" + Timestamp((DateTime)curr.Value) + "'";
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                keyValueClause += curr.Key + "=N'" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                            else
                            {
                                keyValueClause += curr.Key + "='" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                        }
                    }
                    else
                    {
                        keyValueClause += curr.Key + "= null";
                    }
                }
                else
                {
                    if (curr.Value != null)
                    {
                        if (curr.Value is DateTime || curr.Value is DateTime?)
                        { 
                            keyValueClause += "," + curr.Key + "='" + Timestamp((DateTime)curr.Value) + "'";
                        }
                        else
                        {
                            if (Helper.IsExtendedCharacters(curr.Value.ToString()))
                            {
                                keyValueClause += "," + curr.Key + "=N'" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                            else
                            {
                                keyValueClause += "," + curr.Key + "='" + SanitizeString(curr.Value.ToString()) + "'";
                            }
                        }
                    }
                    else
                    {
                        keyValueClause += "," + curr.Key + "= null";
                    }
                }
                added++;
            }

            #endregion

            #region Build-UPDATE-Query-and-Submit
             
            query += "UPDATE " + tableName + " SET ";
            query += keyValueClause + " ";
            if (filter != null) query += "WHERE " + filter.ToWhereClause() + " ";
            result = Query(query);

            #endregion

            return;
        }

        /// <summary>
        /// Execute a DELETE query.
        /// </summary>
        /// <param name="tableName">The table in which you wish to DELETE.</param>
        /// <param name="filter">The expression containing the DELETE filter (i.e. WHERE clause data).</param>
        public void Delete(string tableName, Expression filter)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (filter == null) throw new ArgumentNullException(nameof(filter));

            string query = "";
            DataTable result;

            #region Build-DELETE-Query-and-Submit
             
            query += "DELETE FROM " + tableName + " ";
            if (filter != null) query += "WHERE " + filter.ToWhereClause() + " ";
                   
            result = Query(query);

            #endregion

            return;
        }

        #endregion

        #region Private-Methods

        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            {
                if (_Connection != null)
                {
                    _Connection.Dispose();
                    if (_Connection.State == ConnectionState.Open) _Connection.Close();
                }
            }

            _Disposed = true;

            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        private void CreateFile(string filename)
        {
            if (!File.Exists(filename))
            {
                SqliteConnection.CreateFile(filename);
            }
        }

        private void BuildConnectionString()
        {
            _ConnectionString = "Data Source=" + _Filename + ";Version=3;";
        }

        private void Connect()
        {
            _Connection = new SqliteConnection(_ConnectionString);
            _Connection.Open();
        }

        #endregion
    }
}