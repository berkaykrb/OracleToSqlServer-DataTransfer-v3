using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using Topshelf;

namespace dataIntegration
{
    public class TableColumn
    {
        public string ColumnName { get; set; }
        public string DataType { get; set; }
    }

    public class NewTable
    {
        public string[,] Columns { get; set; }
        public string SourceTableName { get; set; }
        public string TargetTableName { get; set; }
        public string SqlQuery { get; set; }
        public bool DefaultQuery { get; set; }
    }

    class Program
    {
        private static Timer timer;
        public static string orcl_provider = "Provider=OraOLEDB.Oracle; Data Source=(DESCRIPTION=(CID=GTU_APP)(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP) (HOST=XXXXXXXXXXX)(PORT=XXXXXXXXXXX)))(CONNECT_DATA=(SID=XXXXXXXXXXX)(SERVER=DEDICATED))); User Id=XXXXXXXXXXX; Password=XXXXXXXXXXX;";
        public static string sql_provider = "Server=XXXXXXXXXXX;Database=XXXXXXXXXXX;User Id=XXXXXXXXXXX;Password=XXXXXXXXXXX; ";

        public class dataIntegration
        {
            public dataIntegration()
            {

            }

            public void Start()
            {

                Console.WriteLine("##########################");
                Console.WriteLine("#    Data Integration    #");
                Console.WriteLine("#        by berk         #");
                Console.WriteLine("#          v3.0          #");
                Console.WriteLine("##########################");
                Console.WriteLine("");

                firstTransactions();
                timer = new Timer(timer_Elapsed);
                timer.Change(600000, 600000);
                Console.Read();

            }

            public void Stop()
            {

            }
        }

        static void Main(string[] args)
        {

            var rc = HostFactory.Run(x =>
            {
                x.Service<dataIntegration>(s =>
                {
                    s.ConstructUsing(name => new dataIntegration());
                    s.WhenStarted(tc => tc.Start());
                    s.WhenStopped(tc => tc.Stop());
                });
                x.RunAsLocalSystem();

                x.SetDescription("Data Integration v3.0");
                x.SetDisplayName("dataIntegrationService");
                x.SetServiceName("dataIntegrationService");
            });

        }

        public static bool checkTime()
        {
            string hour = DateTime.Now.ToString("HH");
            int min = Int32.Parse(DateTime.Now.ToString("mm"));
            return hour == "08" || (hour == "17" && (min > 45 || min < 56));
        }

        private static void firstTransactions()
        {

            getTablesFromJSON();

            if (checkTime())
            {

                getTablesFromJSON("timeLimitedTransfer");

            }
        }

        private static void timer_Elapsed(object o)
        {

            getTablesFromJSON();
            
            if (checkTime())
            {

                getTablesFromJSON("timeLimitedTransfer");

            }

        }
        public static void getTablesFromJSON(string transferType = "normalTransfer")
        {

            List<NewTableRequest> tableList = new List<NewTableRequest>();

            string jsonFilesPath;

            if (transferType == "timeLimitedTransfer") {
                 jsonFilesPath = "tableDefinitions\\timeLimitedTransfers\\";
            }
            else
            {
                 jsonFilesPath = "tableDefinitions\\";
            }
            string[] jsonFiles = Directory.GetFiles(jsonFilesPath, "*.json");

            foreach (string jsonFilePath in jsonFiles)
            {
                string jsonText = File.ReadAllText(jsonFilePath);

                NewTable root = JsonConvert.DeserializeObject<NewTable>(jsonText);

                string[,] columns = root.Columns;

                NewTableRequest newTable = new NewTableRequest
                {
                    sourceTableName = root.SourceTableName,
                    targetTableName = root.TargetTableName,
                    columns = columns,
                    sqlQuery = root.SqlQuery,
                    defaultQuery = true
                };

                tableList.Add(newTable);
            }

            transferData(tableList);
        }


        public static void transferData(List<NewTableRequest> tableDetailsList)
        {
            foreach (var tableDetails in tableDetailsList)
            {

                try
                {

                    Console.WriteLine("(" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ") | [" + tableDetails.targetTableName + "] Data transfer process started.");

                    var orclsqlCon = new OleDbConnection(orcl_provider);
                    orclsqlCon.Open();
                    String SQL = "";

                    if (tableDetails.defaultQuery)
                    {
                        SQL = " SELECT * FROM " + tableDetails.sourceTableName + " ";
                    }
                    else
                    {
                        SQL = tableDetails.sqlQuery;
                    }

                    OleDbCommand CMD = new OleDbCommand(SQL, orclsqlCon);
                    OleDbDataReader dr = CMD.ExecuteReader();

                    dr.Read();
                    if (dr.HasRows)
                    {

                        DataTable veriTbl = new DataTable();
                        String[] columnNames = { };
                        Type[] columnTypes = { };

                        for (int i = 0; i < tableDetails.columns.GetLength(0); i++)
                        {
                            if (tableDetails.columns[i, 1] == "Decimal")
                            {
                                veriTbl.Columns.Add(new DataColumn(tableDetails.columns[i, 0], typeof(Decimal)));
                            }
                            if (tableDetails.columns[i, 1] == "String")
                            {
                                veriTbl.Columns.Add(new DataColumn(tableDetails.columns[i, 0], typeof(String)));
                            }
                        }

                        veriTbl.Load(dr);

                        SqlConnection con = new SqlConnection(sql_provider);
                        SqlConnection con2 = new SqlConnection(sql_provider);

                        string sqlStatement = "DELETE FROM " + tableDetails.targetTableName + " ";

                        con2.Open();
                        SqlCommand cmd2 = new SqlCommand(sqlStatement, con2);
                        cmd2.CommandType = CommandType.Text;
                        cmd2.ExecuteNonQuery();
                        con2.Close();

                        SqlBulkCopy veriBulk = new SqlBulkCopy(con);

                        veriBulk.DestinationTableName = tableDetails.targetTableName;

                        for (int i = 0; i < tableDetails.columns.GetLength(0); i++)
                        {
                            veriBulk.ColumnMappings.Add(tableDetails.columns[i, 0], tableDetails.columns[i, 0]);
                        }


                        con.Open();

                        veriBulk.BulkCopyTimeout = 0;
                        veriBulk.WriteToServer(veriTbl);

                        con.Close();
                        veriTbl.Clear();
                        veriBulk.Close();

                    }

                    else
                    {
                        Console.WriteLine("(" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ") | [" + tableDetails.targetTableName + "] There is no data to transfer. ");
                    }

                    orclsqlCon.Close();

                    Console.WriteLine("(" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ") | [" + tableDetails.targetTableName + "] Data transferred successfully.");

                }

                catch (Exception e)
                {

                    Console.WriteLine("(" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ") | [" + tableDetails.targetTableName + "] An error occurred while transferring data.");
                    Console.WriteLine("(" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + ") | [" + tableDetails.targetTableName + "] Error: " + e.Message.ToString());

                }
            }

        }
    }
}
