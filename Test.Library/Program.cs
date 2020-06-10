using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using Newtonsoft.Json;
using Microsoft.Data.Sqlite;

namespace Test.Library
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string connStr = "Data Source=test.db";

                string tableQuery = "CREATE TABLE IF NOT EXISTS 'company' (id Integer PRIMARY KEY AUTOINCREMENT NOT NULL , name Text COLLATE NOCASE , postal Integer);";

                /*
                 * Fails
                string metadataQuery = "SELECT t.name AS tbl_name, c.name, c.type, c.[notnull], c.dflt_value, c.pk FROM sqlite_master AS t, pragma_table_info(t.name) AS c WHERE t.type = 'table';";
                 */

                /*
                 * Works
                string metadataQuery = "DROP TABLE IF EXISTS info;" +
                       "CREATE TEMPORARY TABLE info AS SELECT t.name AS tbl_name, c.name, c.type, c.[notnull], c.dflt_value, c.pk FROM sqlite_master AS t, pragma_table_info(t.name) AS c WHERE t.type = 'table';" +
                       "SELECT * FROM  info";
                 */

                string metadataQuery =
                    "DROP TABLE IF EXISTS tmpinfo; " +
                    "CREATE TEMPORARY TABLE tmpinfo AS " +
                    "SELECT " +
                    "    m.name AS TABLE_NAME,  " +
                    "    p.cid AS COLUMN_ID, " +
                    "    p.name AS COLUMN_NAME, " +
                    "    p.type AS DATA_TYPE, " +
                    "    p.pk AS IS_PRIMARY_KEY, " +
                    "    p.[notnull] AS IS_NOT_NULLABLE " +
                    "FROM sqlite_master m " +
                    "LEFT OUTER JOIN pragma_table_info(m.name) p " +
                    "    ON m.name <> p.name " +
                    "WHERE m.type = 'table' " +
                    "    AND m.name = 'company' " +
                    "ORDER BY TABLE_NAME, COLUMN_ID; " +
                    "SELECT * FROM tmpinfo;";

                using (SqliteConnection conn = new SqliteConnection(connStr))
                {
                    Console.WriteLine(conn.ServerVersion);
                    conn.Open();
                    DataTable result = Query(tableQuery, conn);

                    result = Query(metadataQuery, conn);
                    Console.WriteLine(SerializeJson(DataTableToListDynamic(result), true));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            Console.ReadLine();
        }

        private static DataTable Query(string query, SqliteConnection conn)
        {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
            using (SqliteCommand cmd = new SqliteCommand(query, conn))
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
            {
                using (SqliteDataReader rdr = cmd.ExecuteReader())
                {
                    DataTable result = new DataTable();
                    result.Load(rdr);
                    return result;
                }
            }
        }

        private static string SerializeJson(object obj, bool pretty)
        {
            if (obj == null) return null;
            string json;

            if (pretty)
            {
                json = JsonConvert.SerializeObject(
                  obj,
                  Newtonsoft.Json.Formatting.Indented,
                  new JsonSerializerSettings
                  {
                      NullValueHandling = NullValueHandling.Ignore,
                      DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                  });
            }
            else
            {
                json = JsonConvert.SerializeObject(obj,
                  new JsonSerializerSettings
                  {
                      NullValueHandling = NullValueHandling.Ignore,
                      DateTimeZoneHandling = DateTimeZoneHandling.Utc
                  });
            }

            return json;
        }

        private static List<dynamic> DataTableToListDynamic(DataTable dt)
        {
            List<dynamic> ret = new List<dynamic>();
            if (dt == null || dt.Rows.Count < 1) return ret;

            foreach (DataRow curr in dt.Rows)
            {
                dynamic dyn = new ExpandoObject();
                foreach (DataColumn col in dt.Columns)
                {
                    var dic = (IDictionary<string, object>)dyn;
                    dic[col.ColumnName] = curr[col];
                }
                ret.Add(dyn);
            }

            return ret;
        }

        private static dynamic DataTableToDynamic(DataTable dt)
        {
            dynamic ret = new ExpandoObject();
            if (dt == null || dt.Rows.Count < 1) return ret;

            foreach (DataRow curr in dt.Rows)
            {
                foreach (DataColumn col in dt.Columns)
                {
                    var dic = (IDictionary<string, object>)ret;
                    dic[col.ColumnName] = curr[col];
                }

                return ret;
            }

            return ret;
        }

    }
}
