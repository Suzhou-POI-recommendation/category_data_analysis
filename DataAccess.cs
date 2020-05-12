using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.OleDb;
using MySql.Data.MySqlClient;

namespace category_data_analysis
{
    class DataAccess
    {
        //返回最新的数据库
        public static string GetConnectionString()
        {
            return "Data Source=127.0.0.1;Database=poi;User ID=root;Password=YYhh220929";
            //return "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=127.0.0.1;Database=poi;User ID=root;Jet OLEDB:Database Password=YYhh220929";
        }
        //返回之前错误的数据库，因为有时间数据
        public static string GetConnectionString2()
        {
            return "Data Source=127.0.0.1;Database=poi;User ID=root;Password=YYhh220929";
            //return "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=127.0.0.1;Database=poi;User ID=root;Jet OLEDB:Database Password=YYhh220929";
        }
        //------------------------------------------------------------------------------------------
        //连接模式；通用OLE DB；查询数据
        //------------------------------------------------------------------------------------------
        public static OleDbDataReader ExecuteReader(string connectionStringStr, string sqlStr)
        {
            OleDbConnection cnn = null;
            OleDbCommand cmd = null;
            OleDbDataReader dr = null;

            try
            {
                cnn = new OleDbConnection(connectionStringStr);
                cnn.Open();

                cmd = new OleDbCommand(sqlStr, cnn);
                dr = cmd.ExecuteReader();                

            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.Message);
                return null;
            }
           
            return dr;

        }

        //------------------------------------------------------------------------------------------
        //非连接模式；通用OLE DB；查询数据
        //------------------------------------------------------------------------------------------
        public static DataTable ExecuteQuery(string connectionString, string strSQL)
        {
            OleDbConnection cnn = new OleDbConnection(connectionString);
            DataTable dt = null;

            try
            {
                OleDbDataAdapter adapter = new OleDbDataAdapter(strSQL, cnn);
                DataSet ds = new DataSet();
                if (adapter.Fill(ds) > 0)
                    dt = ds.Tables[0];
            }
            catch (Exception msg)
            {
                System.Console.WriteLine(msg.ToString());
                return dt;
            }

            cnn.Close();

            return dt;

        }

        //------------------------------------------------------------------------------------------
        //连接模式；通用OLE DB；非查询操作
        //------------------------------------------------------------------------------------------
        public static bool ExecuteNoQuery(string connectionStringStr, string sqlStr)
        {
            OleDbConnection cnn = null;
            OleDbCommand cmd = null; 
            
            try
            {
                cnn = new OleDbConnection(connectionStringStr);
                cnn.Open();

                cmd = new OleDbCommand(sqlStr, cnn);
                cmd.ExecuteNonQuery();

            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.Message);
                return false;
            }

            return true;

        }

        //------------------------------------------------------------------------------------------
        //连接模式；MySQL OLE DB；查询操作
        //------------------------------------------------------------------------------------------
        public static MySqlDataReader ExecuteReader2(string connectionStringStr, string strSQL)
        {
            MySqlConnection cnn = null;
            MySqlCommand cmd = null;
            MySqlDataReader rst = null;

            try
            {
                cnn = new MySqlConnection(connectionStringStr);
                cnn.Open();

                cmd = new MySqlCommand();
                cmd.Connection = cnn;
                cmd.CommandText = strSQL;

                rst = cmd.ExecuteReader();

            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.Message);
            }

            //cnn.Close();
            return rst;

        }

        //------------------------------------------------------------------------------------------
        //连接模式；MySQL OLE DB；非查询操作
        //------------------------------------------------------------------------------------------
        public static bool ExecuteNoQuery2(string connectionStringStr, string sqlStr)
        {
            MySqlConnection cnn = null;
            MySqlCommand cmd = null;

            try
            {
                cnn = new MySqlConnection(connectionStringStr);
                cnn.Open();

                cmd = new MySqlCommand(sqlStr, cnn);
                cmd.ExecuteNonQuery();

            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.Message);
                return false;
            }

            return true;

        }
    }

}
