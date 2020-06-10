using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace SqliteHelper
{
    /// <summary>
    /// Lightweight wrapper for Sqlite.
    /// </summary>
    public class DatabaseClient : IDisposable
    {
        #region Public-Members
         
        /// <summary>
        /// Enable or disable logging of queries using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogQueries = false;

        /// <summary>
        /// Enable or disable logging of query results using the Logger(string msg) method (default: false).
        /// </summary>
        public bool LogResults = false;
         
        /// <summary>
        /// Method to invoke when sending a log message.
        /// </summary>
        public Action<string> Logger = null;

        #endregion

        #region Private-Members

        private bool _Disposed = false;
        private string _Filename = null;
        private string _ConnectionString = null;
        private SqliteConnection _Connection = null;
        private readonly object _Lock = new object();

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Initialize Sqlite client using a new file or existing file.
        /// </summary>
        /// <param name="filename">The filename.</param> 
        public DatabaseClient(string filename)
        {
            if (String.IsNullOrEmpty(filename)) throw new ArgumentNullException(nameof(filename));

            _Filename = filename; 
            _ConnectionString = "Data Source=" + _Filename;

            _Connection = new SqliteConnection(_ConnectionString);
            _Connection.Open();
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
        public string SanitizeString(string s)
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
        /// <returns>DataTable containing the query result.</returns>
        public DataTable Query(string query)
        {
            if (String.IsNullOrEmpty(query)) throw new ArgumentNullException(nameof(query));
            DataTable result = new DataTable();

            if (LogQueries && Logger != null) Logger("[Sqlite] Query: " + query);

            try
            {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                using (SqliteCommand cmd = new SqliteCommand(query, _Connection))
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
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
                if (LogResults && Logger != null)
                {
                    if (result != null)
                    {
                        Logger("[Sqlite] Query result: " + result.Rows.Count + " rows");
                    }
                    else
                    {
                        Logger("[Sqlite] Query result: null");
                    }
                }
            }
        }

        /// <summary>
        /// Execute a scalar SQL query (generally multiple statements in a single query).
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns>Object containing result from the query.</returns>
        public object QueryScalar(string query)
        {
            object result = null; 

            if (LogQueries && Logger != null) Logger("[Sqlite] QueryScalar: " + query);

            try
            {
                if (String.IsNullOrEmpty(query)) return false;

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                using (SqliteCommand cmd = new SqliteCommand(query, _Connection))
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                {
                    result = cmd.ExecuteScalar(); 
                    return result;
                }
            }
            finally
            {
                if (LogResults && Logger != null)
                { 
                    if (result != null)
                    {
                        Logger("[Sqlite] QueryScalar result: " + result.ToString());
                    }
                    else
                    {
                        Logger("[Sqlite] QueryScalar result: null");
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
        /// <returns>Object containing last inserted row ID; generally should be cast to an integer.</returns>
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

        /// <summary>
        /// List all tables in the database.
        /// </summary>
        /// <returns>List of strings, each being a table name.</returns>
        public List<string> ListTables()
        {
            string query =
                "DROP TABLE IF EXISTS tablelist; " +
                "CREATE TEMPORARY TABLE tablelist AS " +
                "  SELECT " +
                "    name AS TABLE_NAME " +
                "  FROM " +
                "    sqlite_master " +
                "  WHERE " +
                "    type ='table' AND " +
                "    name NOT LIKE 'sqlite_%'; " +
                "SELECT * FROM tablelist;";

            DataTable result = null;

            lock (_Lock)
            {
                result = Query(query);
            }

            List<string> tableNames = new List<string>();

            if (result != null && result.Rows.Count > 0)
            { 
                foreach (DataRow curr in result.Rows)
                {
                    tableNames.Add(curr["TABLE_NAME"].ToString());
                } 
            }

            return tableNames;
        }

        /// <summary>
        /// Check if a table exists in the database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if exists.</returns>
        public bool TableExists(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            return ListTables().Contains(tableName);
        }

        /// <summary>
        /// Show the columns and column metadata from a specific table.
        /// </summary>
        /// <param name="tableName">The table to view.</param>
        /// <returns>A list of column objects.</returns>
        public List<Column> DescribeTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            string query =
                "DROP TABLE IF EXISTS tableinfo; " +
                "CREATE TEMPORARY TABLE tableinfo AS " +
                "  SELECT " +
                "    m.name AS TABLE_NAME,  " + 
                "    p.name AS COLUMN_NAME, " +
                "    p.type AS DATA_TYPE, " +
                "    p.pk AS IS_PRIMARY_KEY, " +
                "    p.[notnull] AS IS_NOT_NULLABLE " +
                "  FROM sqlite_master m " +
                "  LEFT OUTER JOIN pragma_table_info(m.name) p " +
                "    ON m.name <> p.name " +
                "  WHERE m.type = 'table' " +
                "    AND m.name = '" + SanitizeString(tableName) + "' " +
                "  ORDER BY TABLE_NAME; " +
                "SELECT * FROM tableinfo;";

            DataTable result = null;

            lock (_Lock)
            {
                result = Query(query);
            }

            List<Column> columns = new List<Column>();
             
            if (result != null && result.Rows.Count > 0)
            {
                foreach (DataRow currColumn in result.Rows)
                {
                    #region Process-Each-Column
                     
                    Column tempColumn = new Column(); 
                    
                    tempColumn.Name = currColumn["COLUMN_NAME"].ToString(); 
                    tempColumn.Type = DataTypeFromString(currColumn["DATA_TYPE"].ToString());
                    tempColumn.PrimaryKey = Convert.ToBoolean(currColumn["IS_PRIMARY_KEY"]);

                    bool isNotNullable = Convert.ToBoolean(currColumn["IS_NOT_NULLABLE"]);
                    tempColumn.Nullable = !isNotNullable;
                     
                    if (!columns.Exists(c => c.Name.Equals(tempColumn.Name)))
                    {
                        columns.Add(tempColumn);
                    }

                    #endregion
                }
            }

            return columns;
        }

        /// <summary>
        /// Describe each of the tables in the database.
        /// </summary>
        /// <returns>Dictionary.  Key is table name, value is List of Column objects.</returns>
        public Dictionary<string, List<Column>> DescribeDatabase()
        {
            DataTable result = new DataTable();
            Dictionary<string, List<Column>> ret = new Dictionary<string, List<Column>>();
            List<string> tableNames = ListTables();

            if (tableNames != null && tableNames.Count > 0)
            {
                foreach (string tableName in tableNames)
                {
                    ret.Add(tableName, DescribeTable(tableName));
                }
            }

            return ret;
        }

        /// <summary>
        /// Create a table with a specified name if it doesn't already exist.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="columns">Columns.</param>
        public void CreateTable(string tableName, List<Column> columns)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (columns == null || columns.Count < 1) throw new ArgumentNullException(nameof(columns));

            string query =
                "CREATE TABLE IF NOT EXISTS '" + SanitizeString(tableName) + "' " +
                "(";

            int columnCount = 0;
            foreach (Column curr in columns)
            {
                if (columnCount > 0) query += ", ";

                query += SanitizeString(curr.Name) + " " + curr.Type.ToString() + " "; 
                if (curr.Type == DataType.Text) query += "COLLATE NOCASE ";

                if (curr.PrimaryKey)
                {
                    query += "PRIMARY KEY ";
                    if (curr.Type == DataType.Integer) query += "AUTOINCREMENT ";
                }

                if (!curr.Nullable) query += "NOT NULL ";

                columnCount++;
            }

            query += 
                ")";

            DataTable result = Query(query);
        }

        /// <summary>
        /// Drop the specified table if it exists.  
        /// </summary>
        /// <param name="tableName">The table to drop.</param>
        public void DropTable(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            string query = "DROP TABLE IF EXISTS '" + SanitizeString(tableName) + "'";
            DataTable result = Query(query);
        }

        /// <summary>
        /// Retrieve the name of the primary key column from a specific table.
        /// </summary>
        /// <param name="tableName">The table of which you want the primary key.</param>
        /// <returns>A string containing the column name.</returns>
        public string GetPrimaryKeyColumn(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details = DescribeTable(tableName);
            if (details != null && details.Count > 0)
            {
                foreach (Column c in details)
                {
                    if (c.PrimaryKey) return c.Name;
                }
            }

            return null;
        }

        /// <summary>
        /// Retrieve a list of the names of columns from within a specific table.
        /// </summary>
        /// <param name="tableName">The table of which ou want to retrieve the list of columns.</param>
        /// <returns>A list of strings containing the column names.</returns>
        public List<string> GetColumnNames(string tableName)
        {
            if (String.IsNullOrEmpty(tableName)) throw new ArgumentNullException(nameof(tableName));

            List<Column> details = DescribeTable(tableName);
            List<string> columnNames = new List<string>();

            if (details != null && details.Count > 0)
            {
                foreach (Column c in details)
                {
                    columnNames.Add(c.Name);
                }
            }

            return columnNames;
        }

        /// <summary>
        /// Retrieve a DataType based on a supplied string.
        /// Refer to https://www.sqlite.org/datatype3.html.
        /// </summary>
        /// <param name="s">String.</param>
        /// <returns>DataType.</returns>
        public DataType DataTypeFromString(string s)
        {
            if (String.IsNullOrEmpty(s)) throw new ArgumentNullException(nameof(s));

            s = s.ToLower();

            if (s.Contains("int")) return DataType.Integer;

            if (s.Contains("char")
                || s.Contains("text")
                || s.Contains("clob")) return DataType.Text;

            if (s.Contains("blob")) return DataType.Blob;

            if (s.Contains("real")
                || s.Contains("double")
                || s.Contains("float")) return DataType.Real;

            if (s.Contains("numeric") ||
                s.Contains("decimal") ||
                s.Contains("bool") ||
                s.Contains("date")) return DataType.Numeric; 

            throw new ArgumentException("Unknown DataType: " + s);
        }

        #endregion

        #region Private-Methods

        /// <summary>
        /// Dispose of resources.
        /// </summary>
        /// <param name="disposing">Disposing.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (_Disposed)
            {
                return;
            }

            if (disposing)
            {
                try
                {
                    if (_Connection != null)
                    {
                        _Connection.Close();
                        _Connection.Dispose();
                    }
                }
                catch (Exception)
                {

                }
            }

            _Disposed = true;

            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
         
        #endregion
    }
}
