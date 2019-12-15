using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;

using Newtonsoft.Json;

using SqliteWrapper;

namespace Test.Dispose
{
    class Program
    {
        static bool _RunForever = true;
        static DatabaseClient _Database;

        static void Main(string[] args)
        {
            Console.WriteLine("Opening file");
            _Database = new DatabaseClient("test");

            Console.WriteLine("Creating table 'company' with fields 'id' (int) and 'val' (string)...");
            string createTableQuery =
                "CREATE TABLE IF NOT EXISTS company " +
                "( id  INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "  val NVARCHAR(64))";
            DataTable createTableResult = _Database.Query(createTableQuery);

            while (_RunForever)
            {
                string userInput = InputString("Command [? for help]:", null, false);
                switch (userInput)
                {
                    case "?":
                        Menu();
                        break;

                    case "c":
                    case "cls":
                        Console.Clear();
                        break;

                    case "q":
                        _RunForever = false;
                        break;

                    case "dispose":
                        _Database.Dispose();
                        break;

                    case "delete":
                        File.Delete("test");
                        break;

                    case "init":
                        _Database = new DatabaseClient("test");
                        break;

                    default:
                        DataTable result = _Database.Query(userInput);
                        if (result != null)
                        {
                            if (result.Rows != null && result.Rows.Count > 0)
                            {
                                if (result.Rows.Count > 1)
                                {
                                    Console.WriteLine(SerializeJson(DataTableToListDynamic(result), true));
                                }
                                else 
                                {
                                    Console.WriteLine(SerializeJson(DataTableToDynamic(result), true));
                                }
                            }
                            else
                            {
                                Console.WriteLine("No rows returned");
                            }
                        }
                        else
                        {
                            Console.WriteLine("No table returned");
                        }
                        break;
                }
            }
        }

        static void Menu()
        {
            Console.WriteLine("-- Available Commands --");
            Console.WriteLine("   ?         Help, this menu");
            Console.WriteLine("   cls       Clear the screen");
            Console.WriteLine("   q         Quit");
            Console.WriteLine("   <query>   Execute a query");
            Console.WriteLine("   dispose   Dispose of the DatabaseClient");
            Console.WriteLine("   delete    Delete the database file");
            Console.WriteLine("   init      Initialize the DatabaseClient");
            Console.WriteLine("");
        }
         
        static string SerializeJson(object obj, bool pretty)
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

        static bool InputBoolean(string question, bool yesDefault)
        {
            Console.Write(question);

            if (yesDefault) Console.Write(" [Y/n]? ");
            else Console.Write(" [y/N]? ");

            string userInput = Console.ReadLine();

            if (String.IsNullOrEmpty(userInput))
            {
                if (yesDefault) return true;
                return false;
            }

            userInput = userInput.ToLower();

            if (yesDefault)
            {
                if (
                    (String.Compare(userInput, "n") == 0)
                    || (String.Compare(userInput, "no") == 0)
                   )
                {
                    return false;
                }

                return true;
            }
            else
            {
                if (
                    (String.Compare(userInput, "y") == 0)
                    || (String.Compare(userInput, "yes") == 0)
                   )
                {
                    return true;
                }

                return false;
            }
        }

        static string InputString(string question, string defaultAnswer, bool allowNull)
        {
            while (true)
            {
                Console.Write(question);

                if (!String.IsNullOrEmpty(defaultAnswer))
                {
                    Console.Write(" [" + defaultAnswer + "]");
                }

                Console.Write(" ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    if (!String.IsNullOrEmpty(defaultAnswer)) return defaultAnswer;
                    if (allowNull) return null;
                    else continue;
                }

                return userInput;
            }
        }

        static List<string> InputStringList(string question, bool allowEmpty)
        {
            List<string> ret = new List<string>();

            while (true)
            {
                Console.Write(question);

                Console.Write(" ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    if (ret.Count < 1 && !allowEmpty) continue;
                    return ret;
                }

                ret.Add(userInput);
            }
        }

        static int InputInteger(string question, int defaultAnswer, bool positiveOnly, bool allowZero)
        {
            while (true)
            {
                Console.Write(question);
                Console.Write(" [" + defaultAnswer + "] ");

                string userInput = Console.ReadLine();

                if (String.IsNullOrEmpty(userInput))
                {
                    return defaultAnswer;
                }

                int ret = 0;
                if (!Int32.TryParse(userInput, out ret))
                {
                    Console.WriteLine("Please enter a valid integer.");
                    continue;
                }

                if (ret == 0)
                {
                    if (allowZero)
                    {
                        return 0;
                    }
                }

                if (ret < 0)
                {
                    if (positiveOnly)
                    {
                        Console.WriteLine("Please enter a value greater than zero.");
                        continue;
                    }
                }

                return ret;
            }
        }
        
        static dynamic DataTableToDynamic(DataTable dt)
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

        static List<dynamic> DataTableToListDynamic(DataTable dt)
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
    }
}
