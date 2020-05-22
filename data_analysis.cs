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
            int month = -1;
            while (dr.Read())
            {

                date = Convert.ToDateTime(dr[0]);
                //Console.WriteLine(date.ToString());
                month = date.Month;
                if (month <= 0) Console.WriteLine("月份读取错误");
                else checkin_by_month[month]++;
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

         public static void form_table()
         {
             string strSQL = String.Format("SELECT poiid FROM pois_suzhou LIMIT 11740,150");
             MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

             //通过非连接模式，需要修改provider
             //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
             //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

             if (dr == null || !dr.Read())
             {
                 Console.WriteLine("数据读取错误");
             }
             else
             {
                 do
                 {
                    string id = Convert.ToString(dr[0]);
                    Console.WriteLine(id);
                     float gender = GenderRatio(id);
                     float local = LocalRatio(id);
                    Console.WriteLine("{0},{1}",gender,local);
                    string strSQL1 = String.Format("INSERT INTO pois_characteristic(poiid,gender_ratio,local_ratio) VALUES(\"{0}\",\"{1}\",\"{2}\")", id,gender,local);
                    MySqlDataReader dr1 = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL1);
                } while (dr.Read());
             }
         }

        public static void form_table1()//对所有poi运算
        {
            string strSQL = String.Format("SELECT poiid FROM pois_suzhou LIMIT 4, 2");
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
            }       
                do
                {
                     Console.WriteLine(Convert.ToString(dr[0]));
                //float gender = GenderRatio(Convert.ToString(dr[0]));
                //float local = LocalRatio(Convert.ToString(dr[0]));
                string id = Convert.ToString(dr[0]);
                    string strSQL1 = String.Format("INSERT INTO poi_characteristic(poiid) VALUES(\"{0}\")",id);
                    MySqlDataReader dr1 = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL1);
                } while (dr.Read());
            
        }


        public static void form_table2()
        {
            string strSQL = String.Format("SELECT poiid FROM pois_suzhou WHERE checkin_num>0 LIMIT 234444,200");//仅对有人打卡的poi运算
            MySqlDataReader dr = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL);

            //通过非连接模式，需要修改provider
            //DataTable result_table = DataAccess.ExecuteQuery(DataAccess.GetConnectionString(), strSQL);
            //return Convert.ToSingle(result_table.Rows[0][0]) / Convert.ToSingle(result_table.Rows[0][1]);

            if (dr == null || !dr.Read())
            {
                Console.WriteLine("数据读取错误");
            }
            else
            {
                int count = 0;
                do
                {
                    count++;
                    string id = Convert.ToString(dr[0]);
                    Console.WriteLine(id);
                    Console.WriteLine("{0}",count);
                    float gender = GenderRatio(id);
                    float local = LocalRatio(id);
                    Console.WriteLine("{0},{1}", gender, local);
                    string strSQL1 = String.Format("INSERT INTO pois_characteristic(poiid,gender_ratio,local_ratio) VALUES(\"{0}\",\"{1}\",\"{2}\")", id, gender, local);
                    MySqlDataReader dr1 = DataAccess.ExecuteReader2(DataAccess.GetConnectionString(), strSQL1);
                } while (dr.Read());
            }
        }      

    }
}
