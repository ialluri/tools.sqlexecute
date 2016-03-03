using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace codomaticSql
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "Codomatic Stored Procedure Executor";
            Console.WriteLine("This program will execute the .sql files mentioned in the pickup folder.  Works for SQL Server only.");

            Console.Write("Sql server Instance: ");
            dbServer = Console.ReadLine();

            Console.Write("Enter database name: ");
            dbSchema = Console.ReadLine();

            Console.Write("Enter database user name (leave blank for windows authentication): ");
            userName = Console.ReadLine();

            Console.Write("Enter database password: ");
            password = Console.ReadLine();

            Console.Write("Enter pickup folder (if current leave blank): ");
            pickupFolder = Console.ReadLine();


            if (pickupFolder.Length == 0)
                pickupFolder = AppDomain.CurrentDomain.BaseDirectory;

            Codomatic.Data.DBCreator.Providers.SqlServer db
                = new Codomatic.Data.DBCreator.Providers.SqlServer();

            db.DBName = dbSchema;
            db.Password = password;
            db.Server = dbServer;
            db.UserID = userName;
            db.StageServer = dbServer;
            //db.RequiresAPIInstallation = false;

            System.IO.DirectoryInfo d = new System.IO.DirectoryInfo(pickupFolder);

            if (d.Exists)
            {
                foreach (System.IO.FileInfo f in d.GetFiles())
                {
                    if (f.FullName.ToUpper().EndsWith(".SQL"))
                    {
                        System.IO.StreamReader r = 
                            new System.IO.StreamReader(f.FullName);

                        Console.WriteLine("Executing: {0}", f.FullName);

                        try
                        {
                            db.MassageAndExecute(r.ReadToEnd());
                        }
                        catch (Exception ex)
                        {
                            System.IO.StreamWriter rw
                                    = new System.IO.StreamWriter
                                        (System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, 
                                            string.Format("Error{0}.log", DateTime.Now.ToString("yyyyMMMdd"))), true);

                            rw.WriteLine("Error in file: {0}{1}  Exception: {2}",
                                        f.FullName, System.Environment.NewLine, ex.Message);

                            rw.Close();
                            Console.WriteLine("   Exception: {0}", ex.Message);
                        }
                    }
                }
            }
            else
                Console.WriteLine("Cannot find the directory!");
                    
        }

        static string userName = "";
        static string password = "";
        static string dbSchema = "";
        static string dbServer = "";
        static string pickupFolder = "";
        //static string filesExtnToExlcude = "";
    }
}
