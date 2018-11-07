using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using com.hnlzwang.ImportAwareVideoGPS.Tools;

namespace com.hnlzwang.ImportGPSToDB
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string connectionString = Convert.ToString(ConfigurationManager.AppSettings["connectionstring"]);
                string videoPath = Convert.ToString(ConfigurationManager.AppSettings["videopath"]);
                string strFilesPath = Convert.ToString(ConfigurationManager.AppSettings["strFilesPath"]);
                List<string> gpsFiles=new List<string>();
                List<string> vidFiles=new List<string>();
                NPgSqlHelper dataAccess = new NPgSqlHelper();
                dataAccess.SetConnectionString(connectionString);
                if(String.IsNullOrEmpty(videoPath))
                {
                    Console.WriteLine("配置错误：确少视频URL配置。");
                    return;
                }

                if (String.IsNullOrEmpty(strFilesPath))
                {
                    Console.WriteLine("配置错误：确少文件路径配置。");
                    return;
                }
                DirectoryInfo directoryInfo=new DirectoryInfo(strFilesPath);
                FileInfo[] fileInfos = directoryInfo.GetFiles();
                foreach (FileInfo fileInfo in fileInfos)
                {
                    if (fileInfo.Name.ToLower().StartsWith("gps_"))
                    {
                        gpsFiles.Add(fileInfo.Name);
                    }

                    if (fileInfo.Name.ToLower().StartsWith("vid_"))
                    {
                        vidFiles.Add(fileInfo.Name);
                    }
                }
                int i = 1;
                string maxVideoIdSql="select case when max(videoid) is null then 0 else max(videoid) end  from thorhild.t_videoroad";
                DataTable dt=dataAccess.GetDataTable(maxVideoIdSql);
                Int64 newVideoId=0;
                if(newVideoId==0)
                {
                    newVideoId=300;
                }
                foreach (string fileName in vidFiles)
                {
                    string fileFullName = strFilesPath + fileName;
                    using (FileStream vidFile=new FileStream(fileFullName,FileMode.Open,FileAccess.Read))
                    {
                        using (StreamReader vidReader=new StreamReader(vidFile))
                        {
                            string vidName = Convert.ToString(vidReader.ReadLine());
                            i+=1;
                            while (!String.IsNullOrEmpty(vidName))
                            {
                                string[] vidNames = vidName.Replace(".mp4","").Split('_');
                                string strStartTime = vidNames[1].Trim();
                                string strEndTime = vidNames[2].Trim();
                                DateTime startTime=DateTime.ParseExact(strStartTime,"yyyyMMddHHmmss",System.Globalization.CultureInfo.CurrentCulture);
                                DateTime endTime=DateTime.ParseExact(strEndTime,"yyyyMMddHHmmss",System.Globalization.CultureInfo.CurrentCulture);
                                using (FileStream gpsFile=new FileStream(fileFullName.Replace("vid","gps"),FileMode.Open,FileAccess.Read))
                                {
                                    using (StreamReader gpsReader=new StreamReader(gpsFile))
                                    {
                                       
                                        newVideoId+=i;
                                        bool isFirst = true;
                                        string lineGeom = String.Empty;
                                        string timeUtc = String.Empty;
                                        string strGpsRead = gpsReader.ReadLine();
                                        string guid = System.Guid.NewGuid().ToString();
                                        DateTime firstTime=new DateTime();
                                        List<string> sqlLists=new List<string>();
                                        while(!String.IsNullOrWhiteSpace(strGpsRead)&&strGpsRead.Contains(","))
                                        {
                                            string[] strRead = strGpsRead.Split(',');
                                            DateTime currentTime = Convert.ToDateTime(strRead[0]);
                                            if(currentTime>=startTime&&currentTime<=endTime)
                                            {

                                                if(isFirst)
                                                {
                                                    firstTime=Convert.ToDateTime(strRead[0]);
                                                    isFirst=false;
                                                }
                                                TimeSpan timeSpan = currentTime-firstTime;
                                                string tmpLineGeom = strRead[1].Trim(' ')+" "+strRead[2].Trim(' ')+",";
                                                if(Convert.ToDouble(strRead[2])>0.0&&Convert.ToDouble(strRead[1])<0.0)
                                                {
                                                    string insertSql1 = "insert into thorhild.t_videopoint(mrfguid,videoguid,lat,lon,utc,videoid,millisecond,geom) values('"+System.Guid.NewGuid().ToString()+"','"+guid+"',"+strRead[2]+","+strRead[1]+",'"+currentTime+"',"+newVideoId+","+timeSpan.TotalMilliseconds+",st_transform(st_geomfromtext('point("+strRead[1]+" "+strRead[2]+")', 4326),3857))";
                                                    Console.WriteLine(insertSql1);
                                                    lineGeom+=tmpLineGeom;
                                                    //int result = dataAccess.ExecuteNonQuery(insertSql1);
                                                    sqlLists.Add(insertSql1);
                                                }
                                            }
                                            if (currentTime > endTime)
                                            {
                                                break;
                                            }
                                            strGpsRead=gpsReader.ReadLine();
                                        }
                                        if(!String.IsNullOrEmpty(lineGeom))
                                        {
                                            string insertSql2 = "insert into thorhild.t_videoroad(mrfguid,create_by,edit_by,videoid,videodir,videoname,geom) values('"+guid+"','admin','admin',"+newVideoId+",'"+videoPath+"','"+vidName+"',st_transform(st_geomfromtext('linestring("+lineGeom.Trim(',')+")', 4326),3857))";
                                            Console.WriteLine(insertSql2);
                                            //int result = dataAccess.ExecuteNonQuery(insertSql2);
                                            sqlLists.Add(insertSql2);
                                        }

                                        if (sqlLists.Count > 0)
                                        {
                                            int result = dataAccess.ExecuteNonQuery(sqlLists);
                                        }
                                    }
                                }
                                vidName = Convert.ToString(vidReader.ReadLine());
                            }
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.Write(ex);
            }
            
        }
    }
}
