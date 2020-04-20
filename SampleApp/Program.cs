using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqliteHelper;

namespace SampleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Opening file");
            DatabaseClient sql = new DatabaseClient("test");

            sql.Logger = Logger;
            sql.LogQueries = true;
            sql.LogResults = true;

            Console.WriteLine("Creating table...");
            List<Column> columns = new List<Column>();
            columns.Add(new Column("id", true, DataType.Integer, false));
            columns.Add(new Column("name", false, DataType.Text, true));
            columns.Add(new Column("postal", false, DataType.Integer, true));
            sql.CreateTable("company", columns);

            Console.WriteLine("Checking if table 'company' exists: " + sql.TableExists("company"));
            Console.WriteLine("Retrieving list of tables...");
            List<string> tableNames = sql.ListTables();
            if (tableNames != null && tableNames.Count > 0)
            {
                foreach (string curr in tableNames) Console.WriteLine("  " + curr);
            }

            Console.WriteLine("Describing database...");
            Dictionary<string, List<Column>> describeResult = sql.DescribeDatabase();
            if (describeResult != null && describeResult.Count > 0)
            {
                foreach (KeyValuePair<string, List<Column>> curr in describeResult)
                {
                    Console.WriteLine("  Table " + curr.Key);
                    if (curr.Value != null && curr.Value.Count > 0)
                    {
                        foreach (Column col in curr.Value)
                        {
                            Console.WriteLine("    " + col.ToString());
                        }
                    }
                }
            }

            Console.WriteLine("Describing table 'company'...");
            List<Column> cols = sql.DescribeTable("company");
            if (cols != null && cols.Count > 0)
            {
                foreach (Column curr in cols)
                {
                    Console.WriteLine("  " + curr.ToString());
                }
            }

            Console.WriteLine("Adding data...");
            Dictionary<string, object> d1 = new Dictionary<string, object>();
            d1.Add("name", "company 1");
            d1.Add("postal", 95128);
            Dictionary<string, object> d2 = new Dictionary<string, object>();
            d2.Add("name", "company 2");
            d2.Add("postal", 62629);
            Dictionary<string, object> d3 = new Dictionary<string, object>();
            d3.Add("name", "company 3");
            d3.Add("postal", 10101);
            Dictionary<string, object> d4 = new Dictionary<string, object>();
            d4.Add("name", "company 4");
            d4.Add("postal", 90210);

            Console.WriteLine("Created ID: " + sql.Insert("company", d1).ToString());
            Console.WriteLine("Created ID: " + sql.Insert("company", d2).ToString());
            Console.WriteLine("Created ID: " + sql.Insert("company", d3).ToString());
            Console.WriteLine("Created ID: " + sql.Insert("company", d4).ToString());

            Console.WriteLine("Selecting companies with postal > 70000...");

            Expression eRetrieve1 = new Expression("postal", Operators.GreaterThan, 70000);
            DataTable selectResult1 = sql.Select("company", 0, null, null, eRetrieve1, null);
            Console.WriteLine("Retrieved " + selectResult1.Rows.Count + " rows");

            Console.WriteLine("Selecting companies with postal > 70000 or postal < 50000...");
            Expression eRetrieve2 = new Expression
            {
                LeftTerm = new Expression("postal", Operators.GreaterThan, 70000),
                Operator = Operators.Or,
                RightTerm = new Expression("postal", Operators.LessThan, 50000)
            };

            DataTable selectResult2 = sql.Select("company", 0, null, null, eRetrieve2, null);
            Console.WriteLine("Retrieved " + selectResult2.Rows.Count + " rows");

            Console.WriteLine("Deleting records...");
            Expression eDelete = new Expression("id", Operators.GreaterThan, 0);
            sql.Delete("company", eDelete);

            Console.WriteLine("Dropping table...");
            sql.DropTable("company");
        }

        private static void Logger(string msg)
        {
            Console.WriteLine(msg);
        }
    }
}
