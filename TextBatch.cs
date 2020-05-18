using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using MySql.Data.MySqlClient.Memcached;
using MySqlX.XDevAPI.Common;
using Newtonsoft.Json;
using TencentCloud.Common;
using TencentCloud.Common.Profile;
using TencentCloud.Mvj.V20190926.Models;
using TencentCloud.Nlp.V20190408;
using TencentCloud.Nlp.V20190408.Models;

namespace category_data_analysis
{

    class TextBatch
    {
        private readonly string connectionString;
        private readonly NlpClient client;
        public static Credential[] credits = {
            new Credential
            {
                SecretId = "AKIDHK8fxOiR2MYZLrPAdtufS3NyOLTgtiVB",
                SecretKey = "hG8my0vlS6c5TbFmRFxviMq9HMzSeeVA"
            },
            new Credential
            {
                SecretId = "AKIDDDDxReHEgsnSzFPaMs75nukMigMnss0r",
                SecretKey = "jCTUDOIv96pI4ZgW2IOnBNJze4D5j6C1"
            },
            new Credential
            {
                SecretId = "AKIDVUGraAAcupBNK29kmbQM8ex4wYt9t4nG",
                SecretKey = "RcchfOs77rLaqrcIlNm08F7j5D6i4FJ8"
            },
            new Credential
            {
                SecretId = "AKIDxvylGazQuXliPoKtTmnXzJN8WvdFpb19",
                SecretKey = "zh8H9k8h4TqhHdcChuqUqugAUeaJmzxe"
            },
            new Credential
            {
                SecretId = "AKIDYpR2MRF3n4M71rB857sNWO1OZTEjNKQG",
                SecretKey = "C7nBKK3rOemeZCZolnY2GCrITN1ODjyw"
            },
            new Credential
            {
                SecretId = "AKIDNUKwJQXp79vSesFVuGciX5Z5LJ0dFNMo",
                SecretKey = "KOBhzIK8jDRxC9rMZVx82e191dy6yavz"
            },
            new Credential
            {
                SecretId = "AKIDIQeeRQGL0WsGsJTrwpY8scC2bNzJfEB0",
                SecretKey = "wcASTi13uq9D0Wq55qSnOilhPAgyzrYG"
            },
            new Credential
            {
                SecretId = "AKIDFFxNXPMKSKlvWrrD2LN69uFRO4ThfpyX",
                SecretKey = "SesviHZ5MN8cplQp0g54qVw72g4ABycx"
            },
            new Credential
            {
                SecretId = "AKIDJPUO2QXu4yb8QcdCbkeIBoJXhcHfnjQx",
                SecretKey = "RdZEckkupUAalgMuCLEBrGQJwJVRUHo4"
            },
            new Credential
            {
                SecretId = "AKIDreBAvWwsOgiVqrnT8mCPO7MLK3h5PCxQ",
                SecretKey = "NjriJeooa5h5D4hASTKZkLhohJackdFc"
            },
            new Credential
            {
                SecretId = "AKID75E8d8sY6Pn56pRFafTBg0ysrdkppHyP",
                SecretKey = "UH9rKEsIn8c3YRDl4BL7dUWJrACHGz2B"
            },
            new Credential
            {
                SecretId = "AKID3NHyhAd1e4ELFsfNkskLlOkpxPaFfbT9",
                SecretKey = "DCrGRN2EBzaZqrb42j9JDxRHqAledMsc"
            },
            new Credential
            {
                SecretId = "AKIDxgRzNDF9FBrNjqRHadPu972hX9Wf2AWf",
                SecretKey = "1pSqyoNf5KI4bs1tUyCBxT2KYz5uUs4S"
            },
            new Credential
            {
                SecretId = "AKID70jANIn5bqoSqGxNKzOWNxhGXXLAKu4T",
                SecretKey = "4sATZPyEExQdhWyAl8D1XqvsY2zJM9hy"
            },
            new Credential
            {
                SecretId = "AKIDc096nmnzA4kAJARtQerxnOilFfrnttrZ",
                SecretKey = "bx4xZuysVr2yf6HFzSeaHlvpSpvlMipu"
            },
            new Credential
            {
                SecretId = "AKIDGLAl8BqTjiQr445FRM1PWWTGn5PEbIZa",
                SecretKey = "ulERN1iGdisnTbT4UcVDqKhpkSt5ze3F"
            },
            new Credential
            {
                SecretId = "AKIDNWODzVLkhPqKXe7GdldExQbXHK7jKE6j",
                SecretKey = "VjhPvC72jAKXUActbmeHf99uT0HUYQ4V"
            },
            new Credential
            {
                SecretId = "AKIDNUMcbKS6AzZ0WnFD0XqteGbgW53piZU8",
                SecretKey = "WmYVHn32mGT092ey2cS6TemloXt7h14u"
            },
            new Credential
            {
                SecretId = "AKIDMTKHzS5HwKTLBlr8ukl10jCNI1obrFfq",
                SecretKey = "3qQ5cWBZgYAbCXoqukHZbQyHJQKuDF1A"
            },
            new Credential
            {
                SecretId = "AKIDFF0DBs0nyub09TfhQt63ma6OQCiYZPS6",
                SecretKey = "Xoq8BrGPJsBE7D5g06vylJbeOr5XkgMm"
            },
            new Credential
            {
                SecretId = "AKID6ycOt7toTt4yOYaYzvbZKpj5AyAFAGyS",
                SecretKey = "g5EEyRcXTIxMefZBVzCWYk1A3RDjyxiJ"
            },
            new Credential
            {
                SecretId = "AKIDKZuxzkgpSotiGok7sLXS6SmCNHuRkoaA",
                SecretKey = "DAqELCrFbghV1HGy7yYrtgtoSpW4LM6l"
            },
            new Credential
            {
                SecretId = "AKIDa3GH1UcXeU3L0elFDl21ye5FExvc5KL5",
                SecretKey = "b0CDy3ObYDbFcjZDbpmSNjocLMozuksY"
            },
            new Credential
            {
                SecretId = "AKIDs0sTptY5D4fy2q7yd1sRjY58uHWVOEBq",
                SecretKey = "UhdkmRouORFm1zKOBhxexykOUTBXBXcR"
            },
            new Credential
            {
                SecretId = "AKIDiCVUIopZ5ey1oOwnaBZt2zmNoNwL7174",
                SecretKey = "seBSTFzSfplLendV9WQ1LN8e6HCpMh4M"
            },
            new Credential
            {
                SecretId = "AKIDsVsVu5xd6KL5IFhMZv86yHK8gxzp4Pbk",
                SecretKey = "CD298INlSwB9rLZVO5VfkyqSq0DBuNdO"
            },
            new Credential
            {
                SecretId = "AKID0zh8JE6DIjuoRoezf3zqQiBBvOYR8OS4",
                SecretKey = "uXtyl9GURMt98rLYU88wTjivpSO3dwyi"
            },
            new Credential
            {
                SecretId = "AKIDz98tQnAvfFagX9PllWO0RdYZKrFNiv0s",
                SecretKey = "TgdLjBMJ1kWBesugDav44pSr0upZ2Gei"
            },
            new Credential
            {
                SecretId = "AKIDmHgshExWR3sQhggoTyDG3aKnfFagbLwG",
                SecretKey = "FH5tTBeusfneIeAPQAbkdGtUqzqQjmWz"
            },
            new Credential
            {
                SecretId = "AKID898wZE6295qQp3yMDsMdcR8E9tSqjRef",
                SecretKey = "lU3RfiYCfrzvhJl294gGfuztcAXehaIv"
            },
            new Credential
            {
                SecretId = "AKIDECtLzpuhlZy6yq8oukKp66B0c3D7uQe0",
                SecretKey = "FG0Zw1qOyIMyJrNdeOrFNEDwOxFD7l9G"
            },
            new Credential
            {
                SecretId = "AKIDQqKYWpN2asxwfVwmVKuYEhvtlbgbiHn3",
                SecretKey = "8NPaY094mVzDvYNiwurd9VeiJhpxqSot"
            },
            new Credential
            {
                SecretId = "AKIDf5GAtLbzCG3kCOskOkqeCDyEC0WkCNO9",
                SecretKey = "Rr1uW4OREZSEjxhEWYqywcEI6g6sRv87"
            },
            new Credential
            {
                SecretId = "AKIDKSQ7YSsSYNvrWuw2LN7dz4CyUjhN3P5U",
                SecretKey = "6vZw31uXAYcbKxKQGF1WnpaR8YicFdk5"
            }

    };
        //private static int batchSize = 2710510 / credits.Length;
        private static int batchSize = 10000;
        private int batch;
        private static int startPos = credits.Length * batchSize * 7;

        public TextBatch(string connection, int batch)
        {
            connectionString = connection;
            this.batch = batch;
            ClientProfile clientProfile = new ClientProfile();
            HttpProfile httpProfile = new HttpProfile();
            httpProfile.Endpoint = ("nlp.tencentcloudapi.com");
            clientProfile.HttpProfile = httpProfile;
            client = new NlpClient(credits[batch-1], "ap-guangzhou", clientProfile);
        }
        private string ConcatKeywords(Dictionary<string, int> keywordCount)
        {
            string concated = "";
            if (keywordCount.Count == 0)
                return concated;
            int maxCount = keywordCount.Values.Max();
            foreach (var pair in keywordCount)
            {
                if (maxCount <= 1)
                    concated += pair.Key + ",";
                if (maxCount > 1)
                    if (pair.Value > 1)
                        concated += pair.Key + ",";
            }
            int cutCount = 1;
            while(concated.Length > 3500)
            {
                concated = "";
                foreach (var pair in keywordCount)
                    if (pair.Value > cutCount*5)
                        concated += pair.Key + ",";
                cutCount++;
            }
            return concated;
        }

        private static string ClearPunctuation(String oriString)
        {
            int len = oriString.Length;
            char[] s = new char[len];
            int len_new = 0;
            for (int i = 0; i < len; i++)
            {
                char c = oriString[i];
                if (c != '#' && c != '@' && c != '$' && c != '%' && c != '^' && c != '/' && c != '~' && c != '\\'
                    && c != '\'' && c != '\"')
                    s[len_new++] = c;
            }
            return new string(s);
        }

        public void Deploy()
        {
            //微博查询
            string query = "SELECT text, annotation_place_poiid, uid FROM poi.weibo_orderby_poi;";
            MySqlDataReader weiboReader = DataAccess.ExecuteReader2(connectionString, query);
            //Dictionary<string, Dictionary<string, int>> keywordCount = new Dictionary<string, Dictionary<string, int>>();
            //Dictionary<string, string> keywordsFinal = new Dictionary<string, string>();
            //Dictionary<string, double> emotion = new Dictionary<string, double>();//暂时存放
            Dictionary<string, int> keywordsCount = new Dictionary<string, int>();
            double positiveProbably = 0;
            int i = -1;
            string prevPOI = "";
            bool startDispose = false;
            string insertString = "INSERT INTO `poi`.`pois_text_feature` (`poiid`, `keywords`, `positive_prob`) values";
            int poiCount = 0;
            StreamWriter file = new StreamWriter(string.Format("batch{0}.txt", batch));
            double totalWeight = 0;
            //批次处理
            while (weiboReader.Read())
            {
                try
                {
                    i++;
                    if (i < batchSize * (batch - 1) - 1 + startPos)
                        continue;
                    if (i == batchSize * (batch - 1) - 1 + startPos)
                    {
                        prevPOI = weiboReader.GetString(1);
                        continue;
                    }

                    //获取poi
                    string poiID = weiboReader.GetString(1).ToUpper();
                    if ((poiID == prevPOI && !startDispose) || poiID == "")
                        continue;//这个部分应该被上一个batch处理过或者POI为空
                    //进度输出
                    if (i % 100 == 0)
                        Console.WriteLine(i);
                    if (i >= batchSize * batch - 1 + startPos)
                        startDispose = false;

                   
                    if (poiID != prevPOI)
                    {
                        prevPOI = poiID;
                        if (!startDispose && i < batchSize * batch - 1 + startPos)
                            startDispose = true;
                        else
                        {
                            //TODO：构建写入字符串
                            string keywordsString = ConcatKeywords(keywordsCount);
                            keywordsCount.Clear();
                            if (poiCount > 0)
                                insertString += ",\n";
                            //if (positiveProbably < -10000)
                            //    positiveProbably = -10000;
                            //if (positiveProbably > 10000)
                            //    positiveProbably = 10000;
                            if (totalWeight > 0)
                                insertString += string.Format("('{0}', '{1}', '{2}')", poiID, keywordsString, positiveProbably/totalWeight);
                            else
                                insertString += string.Format("('{0}', '{1}', '{2}')", poiID, keywordsString, 0.5);
                            positiveProbably = 0;
                            totalWeight = 0;
                            poiCount++;
                            if (!startDispose && i >= batchSize * batch - 1 + startPos)
                                break;
                            //if (poiCount > 0)
                            //    break;
                            if (poiCount % 10 == 0)
                            {
                                try
                                {
                                    DataAccess.ExecuteNoQuery2(connectionString, insertString);
                                }
                                catch (Exception e)
                                {
                                    Console.Write(e.Message);
                                    Console.WriteLine(String.Format(" in Data {0}", i));
                                    file.Write(e.Message);
                                    file.WriteLine(String.Format(" in Data {0}", i));
                                    Console.WriteLine(insertString);
                                    file.WriteLine(insertString);
                                }
                                poiCount = 0;
                                insertString = "INSERT INTO `poi`.`pois_text_feature` (`poiid`, `keywords`, `positive_prob`) values";
                            }
                        }
                    }
                    // 获取uid
                    string uid = weiboReader.GetString(2);
                    // 获取微博
                    string weiboString = ClearPunctuation(weiboReader.GetString(0));//清除标点
                    if (weiboString == "")
                        continue;
                    string strParams = "";
                    if (weiboString.Contains("http"))
                        strParams = "{\"Text\":\"" + weiboString.Substring(0, weiboString.IndexOf("http")) + "\"}";
                    else
                        strParams = "{\"Text\":\"" + weiboString + "\"}";//处理给API的字符参数

                    //关键词提取
                    KeywordsExtractionRequest keywordsRequest = new KeywordsExtractionRequest();
                    keywordsRequest = KeywordsExtractionRequest.FromJsonString<KeywordsExtractionRequest>(strParams);
                    KeywordsExtractionResponse keywordsResponse = client.KeywordsExtractionSync(keywordsRequest);
                    //词频统计
                    //if (!keywordCount.ContainsKey(poiID) && poiID != "")
                    //{
                    //    keywordCount.Add(poiID, new Dictionary<string, int>());
                    //}
                    foreach (Keyword keyword in keywordsResponse.Keywords)
                    {
                        if (poiID == "")
                            break;
                        if (!keywordsCount.ContainsKey(keyword.Word) && poiID != "")
                            keywordsCount.Add(keyword.Word, 0);
                        keywordsCount[keyword.Word]++;
                    }

                    //情感分析
                    SentimentAnalysisRequest sentimentRequest = new SentimentAnalysisRequest();
                    sentimentRequest = SentimentAnalysisRequest.FromJsonString<SentimentAnalysisRequest>(strParams);
                    SentimentAnalysisResponse sentimentResponse = client.SentimentAnalysisSync(sentimentRequest);
                    MySqlDataReader followerReader = DataAccess.ExecuteReader2(connectionString,
                            "Select followers_count from travel_poi_userinfo_suzhou where id = " + uid);
                    followerReader.Read();
                    double follower_weight = Math.Log10(followerReader.GetInt32(0)+10);
                    totalWeight += follower_weight;
                    positiveProbably += follower_weight * (double)sentimentResponse.Positive;
                    followerReader.Close();

                }
                catch (Exception e)
                {
                    Console.Write(e.Message);
                    Console.WriteLine(String.Format(" in Data {0}", i));
                    file.Write(e.Message);
                    file.WriteLine(String.Format(" in Data {0}", i));
                    //Console.WriteLine(e.StackTrace);

                }
            }
            weiboReader.Close();
            if (startDispose)
                file.WriteLine("----------END----------");
            //数据库写入
            try
            {
                 DataAccess.ExecuteNoQuery2(connectionString, insertString);
            }
            catch (Exception e)
            {
                Console.Write(e.Message);
                Console.WriteLine(String.Format(" in Data {0}", i));
                file.Write(e.Message);
                file.WriteLine(String.Format(" in Data {0}", i));
                Console.WriteLine(insertString);
                file.WriteLine(insertString);
            }

            file.WriteLine("--------------------DONE-------------------");
            file.Close();
        }
    }
}
