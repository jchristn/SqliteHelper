using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqliteWrapper;

namespace SampleApp
{
    class SampleApp
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Opening file");
            DatabaseClient sql = new SqliteWrapper.DatabaseClient("test", true);

            Console.WriteLine("Creating table...");
            string createTableQuery =
                "CREATE TABLE IF NOT EXISTS company " +
                "( id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "  name NVARCHAR(64), " +
                "  postal INT)";
            DataTable createTableResult = sql.Query(createTableQuery);

            Console.WriteLine("Adding data");
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

            Console.WriteLine("Selecting companies with postal > 70000");
            Expression eRetrieve1 = new Expression
            {
                LeftTerm = "postal",
                Operator = Operators.GreaterThan,
                RightTerm = 70000
            };

            DataTable selectResult1 = sql.Select("company", 0, null, null, eRetrieve1, null);
            Console.WriteLine("Retrieved " + selectResult1.Rows.Count + " rows");

            Console.WriteLine("Selecting companies with postal > 70000 or postal < 50000");
            Expression eRetrieve2 = new Expression
            {
                LeftTerm = new Expression
                {
                    LeftTerm = "postal",
                    Operator = Operators.GreaterThan,
                    RightTerm = 70000
                },
                Operator = Operators.Or,
                RightTerm = new Expression
                {
                    LeftTerm = "postal",
                    Operator = Operators.LessThan,
                    RightTerm = 50000
                }
            };

            DataTable selectResult2 = sql.Select("company", 0, null, null, eRetrieve2, null);
            Console.WriteLine("Retrieved " + selectResult2.Rows.Count + " rows");

            Console.WriteLine("Deleting records");
            Expression eDelete = new Expression
            {
                LeftTerm = "id",
                Operator = Operators.GreaterThan,
                RightTerm = 0
            };

            sql.Delete("company", eDelete);
        }
    }
}
