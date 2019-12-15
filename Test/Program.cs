using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqliteWrapper;

namespace Test
{
    class Program
    {
        static bool _RunForever = true;
        static string _Filename;
        static DatabaseClient _Database;

        static void Main(string[] args)
        {
            while (String.IsNullOrEmpty(_Filename))
            {
                Console.Write("Filename: ");
                _Filename = Console.ReadLine();
            }

            _Database = new DatabaseClient(_Filename);

            Console.WriteLine("Type '?' for help.");

            while (_RunForever)
            {
                Console.Write("> ");
                string userInput = Console.ReadLine();
                if (String.IsNullOrEmpty(userInput)) continue;

                switch (userInput)
                {
                    case "?":
                        Console.WriteLine("---");
                        Console.WriteLine("q        quit the application");
                        Console.WriteLine("cls      clear the screen");
                        Console.WriteLine("backup   backup database to another file");
                        Console.WriteLine("scalar   execute a scalar query");
                        Console.WriteLine("<query>  submit query to Sqlite");
                        Console.WriteLine("");
                        break;

                    case "q":
                    case "Q":
                        _RunForever = false;
                        break;

                    case "cls":
                    case "CLS":
                        Console.Clear();
                        break;

                    case "backup":
                        Console.Write("Backup filename > ");
                        userInput = null;
                        while (String.IsNullOrEmpty(userInput)) userInput = Console.ReadLine();
                        _Database.Backup(userInput);
                        break;

                    case "scalar":
                        Console.Write("scalar > ");
                        userInput = null;
                        while (String.IsNullOrEmpty(userInput)) userInput = Console.ReadLine();
                        object ret = null;
                        try
                        {
                            ret = _Database.QueryScalar(userInput);
                        }
                        catch (Exception e1)
                        {
                            Console.WriteLine("Exception: " + e1.Message);
                        }
                        if (ret != null) Console.WriteLine(ret.ToString());
                        else Console.WriteLine("(null)");
                        break;

                    default:
                        DataTable result = null;
                        try
                        {
                            result = _Database.Query(userInput);
                        }
                        catch (Exception e2)
                        {
                            Console.WriteLine("Exception: " + e2.Message);
                        }
                        if (result != null) Console.WriteLine(result.Rows.Count + " rows returned");
                        else Console.WriteLine("(null)");
                        break;
                }
            }
        }
    }
}
