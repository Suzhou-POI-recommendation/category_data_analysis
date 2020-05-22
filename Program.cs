/**********************************************************************
 * Project:苏州城市热点POI推荐系统
 * @file:category_data_analysis
 * @author:zwy,yf
 * @说明:实现原始数据中类别信息的整理，包括性别比例、本地用户比例、时间统计、签到大v数量、对签到人数和签到次数分类五个方面
 * 
 * 
 * *********************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace category_data_analysis
{
    class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine(data_analysis.CalcRatio("B2094757D164A2FE4093"));
           // Console.WriteLine(data_analysis.CalcTime("B2094650D064A0F5469D"));
            Console.WriteLine(data_analysis.GenderRatio("B2094650D064A0F8489B"));
            Console.WriteLine(data_analysis.LocalRatio("B2094650D064A0F8489B"));
            Console.WriteLine(data_analysis.VerifiedRatio("B2094650D064A0F8489B"));
            Console.WriteLine("这个语句的意义是防止控制台自动退出");
        }
    }
}
