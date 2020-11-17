using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Wenli.Data.Kafka.Common
{
    /// <summary>
    ///     时间处理类
    ///     服务器时间
    /// </summary>
    public static class DateTimeUtil
    {
        private static DateTime _dt = DateTime.Now;

        static DateTimeUtil()
        {
            var td = new Thread(new ThreadStart(() =>
            {
                while (true)
                {
                    _dt = DateTime.Now;
                    Thread.Sleep(1);
                }
            }));
            td.IsBackground = true;
            td.Start();
        }


        /// <summary>
        /// 当前时间
        /// </summary>
        public static DateTime CurrentDateTime
        {
            get
            {
                return _dt;
            }
        }
        /// <summary>
        /// 当前时间字符串
        /// </summary>
        public static string CurrentDateTimeString
        {
            get
            {
                return _dt.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);
            }
        }
        /// <summary>
        /// Token所需时间
        /// 5分种间隔的时间
        /// </summary>
        public static DateTime TokenDateTime
        {
            get
            {
                int nm = 0;

                DateTime dt = DateTimeUtil.CurrentDateTime;

                int m = dt.Minute;

                //个位分钟数
                int s = (int)(((m * 0.1) - Math.Floor(m * 0.1)) * 10);

                if (s < 5)
                {
                    nm = m - s;
                }
                else
                {
                    nm = m - s + 5;
                }

                return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, nm, 0);
            }
        }
        /// <summary>
        /// 返回当前时间的总秒数
        /// </summary>
        /// <returns></returns>
        public static int CurrentTotalSeconds()
        {
            var ticks = DateTimeUtil.CurrentDateTime.Ticks;
            var ts = new TimeSpan(ticks);
            return (int)ts.TotalSeconds;
        }

        /// <summary>
        /// 获取linux时间ticks
        /// </summary>
        /// <returns></returns>
        public static long LinuxDateTimeTicks()
        {
            return _dt.ToFileTimeUtc();
        }

        /// <summary>
        /// 将CSharp的ticks转换成
        /// </summary>
        /// <param name="ticks"></param>
        /// <returns></returns>
        public static long ConvertToLinuxTicks(long ticks)
        {
            var dt = new DateTime(ticks);
            return dt.ToFileTimeUtc();
        }

        public static long ConvertToCSharpTicks(long ticks)
        {
            return DateTime.FromFileTimeUtc(ticks).Ticks;
        }

        /// <summary>
        /// 时间戳转为C#格式时间
        /// </summary>
        /// <param name=”timeStamp”></param>
        /// <returns></returns>
        public static DateTime GetTime(long timeStamp)
        {
            DateTime dtStart = TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
            long lTime = long.Parse(timeStamp + "0000000");
            TimeSpan toNow = new TimeSpan(lTime);
            return dtStart.Add(toNow);
        }

        /// <summary>
        /// DateTime时间格式转换为Unix时间戳格式
        /// </summary>
        /// <param name=”time”></param>
        /// <returns></returns>
        public static int ConvertDateTimeInt(System.DateTime time)
        {
            System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1));
            return (int)(time - startTime).TotalSeconds;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static int ConvertDateTimeInt(string dateTime)
        {
            DateTime time = DateTime.Now;
            if (!string.IsNullOrEmpty(dateTime))
                time = DateTime.Parse(dateTime);
            System.DateTime startTime = TimeZone.CurrentTimeZone.ToLocalTime(new System.DateTime(1970, 1, 1));
            return (int)(time - startTime).TotalSeconds;
        }

        /// <summary>
        /// 字符串时间类型转换为时间类型
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static DateTime ConvertStrToTime(string dateTime)
        {
            if (!string.IsNullOrEmpty(dateTime))
                return DateTime.Parse(dateTime);
            else
                return DateTime.Now;
        }

        /// <summary>
        /// 转换成yyyy-MM-dd的格式
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public static string ToDateString(this DateTime date)
        {
            return date.ToString("yyyy-MM-dd");
        }

        /// <summary>
        /// 返回日期时间字符串，格式：yyyy-MM-dd HH:mm:ss.fff
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static string ToDateTimeString(this DateTime dateTime)
        {
            return dateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
        }

        /// <summary>
        /// 返回日期时间字符串，格式：yyyy-MM-dd HH:mm:ss.fff
        /// 如果为null，则返回空字符串
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static string ToDateTimeString(this DateTime? dateTime)
        {
            return dateTime.HasValue ? dateTime.Value.ToDateTimeString() : string.Empty;
        }

        /// <summary>
        /// 获取当前的时间int值
        /// </summary>
        /// <returns></returns>
        public static int ConvertDateTimeInt()
        {
            return DateTimeUtil.ConvertDateTimeInt(DateTimeUtil.CurrentDateTime);
        }

        /// <summary>
        /// 返回最后的时间
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static string DateStringFromNow(DateTime dt)
        {
            if (dt == null || dt.ToString("yyyy-MM-dd") == "0001-01-01") return string.Empty;

            TimeSpan span = DateTimeUtil.CurrentDateTime - dt;

            if (span.TotalDays > 60)
            {
                return dt.ToString("yyyy-MM-dd");
            }
            else
            {
                if (span.TotalDays > 30)
                {
                    return
                    "1个月前";
                }
                else
                {
                    if (span.TotalDays > 14)
                    {
                        return
                        "2周前";
                    }
                    else
                    {
                        if (span.TotalDays > 7)
                        {
                            return
                            "1周前";
                        }
                        else
                        {
                            if (span.TotalDays > 1)
                            {
                                return
                                string.Format("{0}天前", (int)Math.Floor(span.TotalDays));
                            }
                            else
                            {
                                if (span.TotalHours > 1)
                                {
                                    return
                                    string.Format("{0}小时前", (int)Math.Floor(span.TotalHours));
                                }
                                else
                                {
                                    if (span.TotalMinutes > 1)
                                    {
                                        return
                                        string.Format("{0}分钟前", (int)Math.Floor(span.TotalMinutes));
                                    }
                                    else
                                    {
                                        if (span.TotalSeconds >= 1)
                                        {
                                            return
                                            string.Format("{0}秒前", (int)Math.Floor(span.TotalSeconds));
                                        }
                                        else
                                        {
                                            return
                                            "1秒前";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 将当前时间字符串转换到unix时间戳,单位秒
        /// </summary>
        /// <returns></returns>
        public static long ConvertCurrentStrToUnixTime()
        {
            DateTime result = DateTimeUtil.CurrentDateTime;
            return result.ToUnixTime();
        }

        /// <summary>
        /// 默认开始时间:1970/1/1
        /// </summary>
        private static DateTime _defaultStartTime = new DateTime(1970, 1, 1);

        /// <summary>
        /// 将时间转换到unix时间戳,单位毫秒
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public static long ToUnixTime(this DateTime dateTime)
        {
            return (long)(dateTime - _defaultStartTime).TotalMilliseconds;
        }
        /// <summary>
        /// 将unix时间戳转换为本地,单位毫秒(与ToUnixTime为互转)
        /// </summary>
        /// <param name="unixTime"></param>
        /// <returns></returns>
        public static DateTime UnixTimeToLocalTime(long unixTime)
        {
            return _defaultStartTime.AddMilliseconds((double)unixTime);
        }
    }
}
