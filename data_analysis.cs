using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace category_data_analysis
{
    class data_analysis
    {
        //计算签到人数与签到次数的比值(数据库中两列数据写反)
        //若搜索次数为0 返回NaN

        public static float CalcRatio(string poi)
        {
            //通过mysql连接模式
            string strSQL = String.Format("SELECT checkin_num,checkin_user_num FROM pois_suzhou WHERE poiid=\"{0}\"", poi);
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
            }

            //多数据处理
            //List<float> ratio_list = new List<float>();
            //while (dr.Read())
            //{
            //    ratio_list.Add(Convert.ToSingle(dr[0]) / Convert.ToSingle(dr[1]));
            //}

            //单数据处理
            return Convert.ToSingle(dr[0]) / Convert.ToSingle(dr[1]);
        }


        //计算签到时间
        //返回长度为12 的数组
        public static int[] CalcTime(string poi)
        {
            int[] checkin_by_month = new int[12];
            //通过mysql连接模式
            string strSQL = String.Format("SELECT time FROM travel_poi_weibos_suzhou WHERE annotation_place_poiid=\"{0}\"", poi);
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString2(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
                return null;
            }
            DateTime date;
            int month=-1;
            while (dr.Read())
            {
                
                date= Convert.ToDateTime(dr[0]);
                //Console.WriteLine(date.ToString());
                month = date.Month;
                if (month <= 0) Console.WriteLine("月份读取错误");
                else checkin_by_month[month]++;
            }

            //单数据处理
            return checkin_by_month;
        }
    }
}
