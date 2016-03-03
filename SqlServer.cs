using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.Sql;
using System.Resources;
using System.Reflection;

namespace Codomatic.Data.DBCreator.Providers
{
    class SqlServer//: DBCreator.IDBCreate 
    {

        #region IDBCreate Members

        #region Stage Server Property
        private string _stageServer = "";
        /// <summary>
        /// The staging Sql Server that hosts user databases
        /// </summary>
        public string StageServer
        {
            get
            {
                return _stageServer;
            }
            set
            {
                _stageServer = value;
            }
        }

        #endregion


        bool connectedToHost = false;
        
        System.Data.SqlClient.SqlConnection hostConnection
            = new System.Data.SqlClient.SqlConnection();

        System.Data.SqlClient.SqlConnection destConnection
            = new System.Data.SqlClient.SqlConnection();

        #region Database Name

        string _database = "";
        /// <summary>
        /// Sql Server Database Name
        /// </summary>
        public string DBName
        {
            get {

                return _database;
            }
            set { _database = value; }
        }

        #endregion

        #region User ID that is created

        string _userID = "";
        /// <summary>
        /// User ID that is created
        /// </summary>
        public string UserID
        {
            get
            {
                return _userID;
            }

            set { _userID = value; }
        }
        #endregion 

        #region Connection String
        /// <summary>
        /// The connection where the customers sql server is hosted on
        /// </summary>
        public string connectionString
        {
            get
            {
                if (UserID == "" || UserID == null)
                    return "Data Source=" + Server + ";" +
                        "Initial Catalog=" + DBName + ";"+
                        "Integrated Security=true;";
                else
                return "Data Source=" + Server + ";" +
                        "Initial Catalog=" + DBName + ";"+
                        "User ID=" + UserID +
                        ";Password=" + Password;
            }            
        }
        #endregion

        #region Password of the user for the customer database

        string _password = "";
        /// <summary>
        /// Password of the customer database
        /// </summary>
        ///        
        public string Password
        {
            get
            {
                return _password;
            }
            set { _password = value; }
        }
        #endregion


        #region Database Server
        string _Server = "";
        public string Server
        {
            get
            {
                return _Server;
            }

            set { _Server = value; }
        }
        #endregion


        public bool DBCreate(bool overwriteObjects, string dbName, string server, string password, string userid, string sql)
        {
            Server = server;
            Password = password;
            UserID = userid;
            DBName = dbName;

            //remove GO statemnt

            char[] s = new char[1];
            s[0] = ((char)210);
            
            string massageSql = sql.Replace(System.Environment.NewLine + "GO" + System.Environment.NewLine, Convert.ToString(s[0]));
            
            string[] massageSqlC = massageSql.Split(s);
            bool returnValue = true;

            if (DBCreate(overwriteObjects, connectionString))
                for (int i = 0; i < massageSqlC.Length; i++)
                {
                    if (!massageSqlC[i].Trim().ToUpper().StartsWith("SET "))
                    {
                        bool x = ExecuteScript(massageSqlC[i]);
                        if (x == false)
                            returnValue = false;
                    }
                }
            else
                return false;

            return returnValue;
        }

        public bool DBCreate(bool overwriteObjects, string connectionString)
        {
            return DBCreate(overwriteObjects);
        }

        public bool DBCreate(bool overwriteObjects)
        {
            System.Data.SqlClient.SqlConnection sqlConnection =
                new System.Data.SqlClient.SqlConnection(StageServer);

            //build a "serverConnection" with the information of the "sqlConnection"
            Microsoft.SqlServer.Management.Common.ServerConnection serverConnection =
              new Microsoft.SqlServer.Management.Common.ServerConnection(sqlConnection);

            //The "serverConnection is used in the ctor of the Server.
            Microsoft.SqlServer.Management.Smo.Server server 
                = new Microsoft.SqlServer.Management.Smo.Server(serverConnection);

            Microsoft.SqlServer.Management.Smo.Database myDb
                = new Microsoft.SqlServer.Management.Smo.Database();

            for (int i = 0; i < server.Databases.Count; i++)
                if (server.Databases[i].Name == DBName)
                {
                    DBDrop();
                    break;
                }

            myDb.Name = DBName;
            myDb.Parent = server;
            myDb.Create();

            string sql1 = "sp_changedbowner '" + UserID + "' ";

            Microsoft.Practices.EnterpriseLibrary.Data.DatabaseFactory.
                CreateDatabase(StageServer.Replace("=master;", "=" + DBName + ";")).ExecuteNonQuery
                (CommandType.Text, sql1);

            return CreateDependencies(ref sql1);

        }

        public bool MassageAndExecute(string sql)
        {

            char[] s = new char[1];
            s[0] = ((char)210);

            sql = sql.Replace(System.Environment.NewLine + "Go" + System.Environment.NewLine, Convert.ToString(s[0]));
            sql = sql.Replace(System.Environment.NewLine + "Go ", Convert.ToString(s[0]));
            sql = sql.Replace(System.Environment.NewLine + "GO ", Convert.ToString(s[0]));
            string massageSql = sql.Replace(System.Environment.NewLine + "GO" + System.Environment.NewLine, Convert.ToString(s[0]));

            string[] massageSqlC = massageSql.Split(s);
            bool returnValue = true;

            for (int i = 0; i < massageSqlC.Length; i++)
            {
                if (!massageSqlC[i].Trim().ToUpper().StartsWith("SET "))
                {
                    bool x = ExecuteScript(massageSqlC[i]);
                    if (x == false)
                        returnValue = false;
                }
            }

            return returnValue;
        }
        public bool ExecuteScript(string sql)
        {
            if (sql == null) return true;
            //System.Data.SqlClient.SqlConnection sqlConnection =
            //   new System.Data.SqlClient.SqlConnection(this.connectionString);

            ////build a "serverConnection" with the information of the "sqlConnection"
            //Microsoft.SqlServer.Management.Common.ServerConnection serverConnection =
            //  new Microsoft.SqlServer.Management.Common.ServerConnection(sqlConnection);

            ////The "serverConnection is used in the ctor of the Server.
            //Microsoft.SqlServer.Management.Smo.Server server
            //    = new Microsoft.SqlServer.Management.Smo.Server(serverConnection);

            //server.sc
            if (sql.Trim().Length == 0) return true;
            try
            {

                //remove GO statemnt

                Microsoft.Practices.EnterpriseLibrary.Data.DatabaseFactory.CreateDatabase(connectionString)
                    .ExecuteNonQuery(CommandType.Text, sql);

                return true;
            }

            catch  (Exception ex)
            
            {
                throw ex;
            }

        }

        public bool dropObjects(string connectionString)
        {
            throw new Exception("The method or operation is not implemented.");
        }

        public bool RequiresAPIInstallation
        {
            get { return true; }
        
        }

        public bool CreateDependencies(ref string ErrorNote)
        {
            //Assembly assembly = this.GetType().Assembly;
            //ResourceManager resMan = new ResourceManager("DBProviderDependencies", assembly);
            //string _APIString = resMan.GetString("SQLServer_DB_Characteristics_API");

            //if (_APIString != null)
            //    if (_APIString.Length > 0)
            //    {
            //        this.ExecuteScript(_APIString);
            //    }

            string sFile = AppDomain.CurrentDomain.BaseDirectory + "DesktopModules/Codomatic.DeveloperDBTools/App_LocalResources/SQLServerApi_Prepare.ctc";

            System.IO.StreamReader r1;
            r1 = System.IO.File.OpenText(sFile);
            string s = r1.ReadToEnd();
            r1.Close();

            if (s != null)
                if (s.Length > 0)
                    ExecuteScript(s);

            sFile = AppDomain.CurrentDomain.BaseDirectory + "DesktopModules/Codomatic.DeveloperDBTools/App_LocalResources/SQLServerApi.ctc";

            System.IO.StreamReader r;
            r = System.IO.File.OpenText(sFile);
            s = "";
            s = r.ReadToEnd();
            r.Close();

            if (s != null)
                if (s.Length > 0)
                    return ExecuteScript(s);

            return false;
        }

        public bool DBDrop()
        {

            System.Data.SqlClient.SqlConnection sqlConnection =
              new System.Data.SqlClient.SqlConnection(StageServer);

            //build a "serverConnection" with the information of the "sqlConnection"
            Microsoft.SqlServer.Management.Common.ServerConnection serverConnection =
              new Microsoft.SqlServer.Management.Common.ServerConnection(sqlConnection);

            
            Microsoft.SqlServer.Management.Smo.Server myServ
            = new Microsoft.SqlServer.Management.Smo.Server(serverConnection);

            Microsoft.SqlServer.Management.Smo.Database myDB
                = new Microsoft.SqlServer.Management.Smo.Database(myServ, DBName);

            myServ.KillAllProcesses(DBName);
            myServ.KillDatabase(DBName);

            return true;
        }

        #endregion
    }
}
