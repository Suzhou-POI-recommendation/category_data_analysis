/**********************************************************************
 * Project:苏州城市热点POI推荐系统
 * @file:category_data_analysis
 * @author:zwy,yf
 * @说明:实现原始数据中类别信息的整理，包括性别、是否本地、时间统计、年龄、对签到人数和签到次数分类五个方面
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
            //Console.WriteLine(data_analysis.CalcRatio("B2094650D16AA0F4459D"));
            Console.WriteLine(data_analysis.CalcTime("B2094650D16AA0F4459D"));
        }
    }
}
