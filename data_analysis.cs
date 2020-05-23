using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.IO;
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
            string strSQL = String.Format("SELECT checkin_num,checkin_user_num FROM pois_suzhou WHERE poiid=\"{0}\"", poi);
            //通过mysql连接模式

            //MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);
            //if (dr == null || !dr.Read())
            //{
            //    Console.WriteLine("数据读取错误");
            //}
            ////单数据处理
            //return Convert.ToSingle(dr[0]) / Convert.ToSingle(dr[1]);

            //通过非连接模式，需要修改provider
            DataTable result_table = DataAccess.ExecuteQuery2(DataAccess.GetConnectionString(), strSQL);
            return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

          

            //多数据处理
            //List<float> ratio_list = new List<float>();
            //while (dr.Read())
            //{
            //    ratio_list.Add(Convert.ToSingle(dr[0]) / Convert.ToSingle(dr[1]));
            //}

            
        }


        //计算签到时间
        //返回长度为12 的数组
        public static int[] CalcTime(string poi)
        {
            int[] checkin_by_month = new int[12];
            //通过mysql连接模式
            string strSQL = String.Format("SELECT time FROM travel_poi_weibos_suzhou,travel_poi_weibos_suzhou_original " +
                "WHERE travel_poi_weibos_suzhou.annotation_place_poiid = \"{0}\" AND travel_poi_weibos_suzhou.id = travel_poi_weibos_suzhou_original.id; ", poi);

            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
                return null;
            }
            DateTime date;
            int month = -1;
            while (dr.Read())
            {

                date = Convert.ToDateTime(dr[0]);
                //Console.WriteLine(date.ToString());
                month = date.Month;
                if (month <= 0) Console.WriteLine("月份读取错误");
                else checkin_by_month[month-1]++;
            }

            //单数据处理
            return checkin_by_month;
        }


        //计算在某个poi处打卡用户的男女比例，女性为0则返回-1；
        //需要先更改mysql的max connections数
        public static float GenderRatio(string poi)
        {
            string strSQL = String.Format("SELECT DISTINCT uid FROM travel_poi_weibos_suzhou WHERE annotation_place_poiid=\"{0}\"", poi);
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
                return -1;
            }

            float m = 0; float f = 0;
            do
            {
                // Console.WriteLine(Convert.ToString(dr[0]));
                string strSQL1 = String.Format("SELECT gender FROM travel_poi_userinfo_suzhou WHERE id=\"{0}\"", dr[0]);
                MySqlDataReader dr1 = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL1);
                if (dr1 == null || !dr1.Read())
                {
                    Console.WriteLine("数据读取错误");
                    return -1;
                }
                //  Console.WriteLine(Convert.ToString(dr1[0]));
                if (Convert.ToString(dr1[0]) == "m")
                    m++;
                else if (Convert.ToString(dr1[0]) == "f")
                    f++;

            }while (dr.Read());   
            Console.WriteLine("M:{0}",m);
            Console.WriteLine("F:{0}", f);
            float all = f + m;
            float ratio = m / all;
            return ratio;
          
        }


        //计算在某个poi处打卡用户的本地比例
        //需要先更改mysql的max connections数
        public static float LocalRatio(string poi)
        {
            string strSQL = String.Format("SELECT DISTINCT uid FROM travel_poi_weibos_suzhou WHERE annotation_place_poiid=\"{0}\"", poi);
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
                return -1;
            }
            float local = 0;float all = 0;
            do
            {
               // Console.WriteLine(Convert.ToString(dr[0]));
                string strSQL1 = String.Format("SELECT province,city FROM travel_poi_userinfo_suzhou WHERE id=\"{0}\"", dr[0]);
                MySqlDataReader dr1 = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL1);
                if (dr1 == null || !dr1.Read())
                {
                    Console.WriteLine("数据读取错误");
                    return -1;
                }
                int province = Convert.ToInt32(dr1[0]);int city = Convert.ToInt32(dr1[1]);
                //Console.WriteLine("{0} {1}",province,city);
                if (province==32 && city==5)
                    local++;
                all++;
            } while (dr.Read());
            Console.WriteLine("Local:{0}", local);
            Console.WriteLine("All:{0}", all);
            float ratio=0;
            ratio = local / all;
            return ratio;

        }


        //计算在某个poi处打卡的名人、达人数量（或比例）
        //需要先更改mysql的max connections数
        public static float VerifiedRatio(string poi)
        {
            string strSQL = String.Format("SELECT DISTINCT uid FROM travel_poi_weibos_suzhou WHERE annotation_place_poiid=\"{0}\"", poi);
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
                return -1;
            }
            int verified = 0;// float all = 0;
            do
            {
                //Console.WriteLine(Convert.ToString(dr[0]));
                string strSQL1 = String.Format("SELECT verified_type FROM travel_poi_userinfo_suzhou WHERE id=\"{0}\"", dr[0]);
                MySqlDataReader dr1 = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL1);
                if (dr1 == null || !dr1.Read())
                {
                    Console.WriteLine("数据读取错误");
                    return -1;
                }
                if (Convert.ToInt32(dr1[0])==0|| Convert.ToInt32(dr1[0]) ==200|| Convert.ToInt32(dr1[0]) ==220)
                    verified++;
               // all++;            
            } while (dr.Read());
            Console.WriteLine("Verified:{0}", verified);
            // Console.WriteLine("All:{0}", all);
           // float ratio = 0;
            // ratio = verified / all;
             //return ratio;
            return verified;
        }



        //导入数据库
        //获得有微博签到的poiid,存储在5个文件中
        public static void GetPoiid()
        {
            //获取前10000个poiid
            string id_query = String.Format("SELECT distinct annotation_place_poiid FROM travel_poi_weibos_suzhou where annotation_place_poiid like \"%B%\";");
            DataTable dt_id = DataAccess.ExecuteQuery2(DataAccess.GetConnectionString(), id_query);
            //string[] ids = new string[10000];
            int file_num = 0;
            int count = 0;//记录行数
            string ids = "";
            while (count < dt_id.Rows.Count)
            {
                for (int i = 0; i < 10000; ++i)
                {
                    if (count < dt_id.Rows.Count)
                    {
                        ids += string.Format("\"{0}\"\n", dt_id.Rows[count][0].ToString());
                        count++;
                    }
                }
                //写入文件
                File.WriteAllText($"id_{file_num}", ids);
                ids = string.Empty;
                file_num++;
            }

        }
        //将性别载入数据库
        public static float GenderRatioToSql()
        {
            string ids = string.Empty;
            string insert_info = "INSERT INTO gender VALUES ";//最终导入时的字符串
            //读取文件
            using (StreamReader reader = new StreamReader("id_1"))
            {
                string tmp_str;//
                try
                {
                    while ((tmp_str = reader.ReadLine()) != null)
                    {
                        ids += tmp_str;
                        ids += ',';
                    }
                    ids = ids.Substring(0, ids.Length - 1);//删除最后一位,
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
            Console.WriteLine("id录入完毕");
            int cursor = 1;//记录读到的ids中的位数
            string id = string.Empty;//表示正在计算的id
            //取出相关微博
            string weibos_sql =
                $"SELECT annotation_place_poiid,uid FROM travel_poi_weibos_suzhou WHERE " +
                $"annotation_place_poiid in ({ids}) order by annotation_place_poiid;";
            DataTable weibos = DataAccess.ExecuteQuery2(DataAccess.GetConnectionString(), weibos_sql);
            MySqlConnection cnn = null;
            cnn = new MySqlConnection(DataAccess.GetConnectionString());
            cnn.Open();
            MySqlCommand cmd = null;
            MySqlDataReader rst = null;
            cmd = new MySqlCommand();
            cmd.Connection = cnn;

            while (cursor <ids.Length)
            {
                id = ids.Substring(cursor, 20);
                Console.WriteLine($"{id}");
                cursor += 23;
                int user_count = 0;
                string poi_user_ids = string.Empty;
                int i = 0;
                while (weibos.Rows[i]["annotation_place_poiid"].ToString() != id) i++;
                while (i < weibos.Rows.Count && weibos.Rows[i]["annotation_place_poiid"].ToString() == id)
                {
                    poi_user_ids += $"\"{ weibos.Rows[i]["uid"].ToString()}\",";

                    user_count++;
                    i++;
                }
                poi_user_ids = poi_user_ids.Substring(0, poi_user_ids.Length - 1);
                string gender_sql = $"SELECT count(*) from travel_poi_userinfo_suzhou " +
                    $"where id in ({poi_user_ids}) and gender=\"m\";";
                try
                {

                    cmd.CommandText = gender_sql;
                    rst = cmd.ExecuteReader();

                }
                catch (Exception e)
                {
                    System.Console.WriteLine(e.Message);
                }
                if (rst.Read())
                {
                    Console.WriteLine(rst[0].ToString());
                    Console.WriteLine(user_count);
                    insert_info += $"(\'{id}\',\'{Convert.ToSingle(rst[0])/user_count}\'),";
                }
                
                rst.Close();
                cmd.Cancel();
            }
            cnn.Close();
            insert_info = insert_info.Substring(0, insert_info.Length - 1);
            DataAccess.ExecuteNoQuery2(DataAccess.GetConnectionString(), insert_info);
            return 0;
        }

        
=======
        
    }
}
