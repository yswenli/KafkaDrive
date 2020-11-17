using System;
using System.Collections.Generic;
using System.Text;

namespace Wenli.Data.Kafka.Common
{
    /// <summary>
    ///     Guid相关操作
    /// </summary>
    public static class GuidUtil
    {
        /// <summary>
        ///     获取新guid字符串，不含有 '-'
        /// </summary>
        public static string GuidString
        {
            get { return Guid.NewGuid().ToString("N"); }
        }

        /// <summary>
        ///     将字符串(不含有'-')转成Guid
        /// </summary>
        /// <param name="guidStr"></param>
        /// <returns></returns>
        public static Guid ConvertToGuid(string guidStr)
        {
            return Guid.ParseExact(guidStr, "N");
        }

        /// <summary>
        ///     将guid字符串(不含有'-')转成数字
        /// </summary>
        /// <param name="guidStr"></param>
        /// <returns></returns>
        public static ulong ConvertToLong(string guidStr)
        {
            var guid = ConvertToGuid(guidStr);
            return ConvertToLong(guid);
        }

        /// <summary>
        /// 将GUID转换成为ulong
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        public static ulong ConvertToLong(Guid guid)
        {
            var buffer = guid.ToByteArray();
            return BitConverter.ToUInt64(buffer, 0);
        }

        /// <summary>
        ///     将数字转成guid字符串
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static string ConvertToStr(ulong data)
        {
            var buffer = BitConverter.GetBytes(data);
            return BitConverter.ToString(buffer);
        }

        /// <summary>
        ///     获取长整形字符串
        /// </summary>
        /// <returns></returns>
        public static ulong GetGuidToLong()
        {
            return ConvertToLong(Guid.NewGuid());
        }

        /// <summary>
        ///     获取字符串型Ulong类型GUID
        /// </summary>
        /// <returns></returns>
        public static string GetGuidToLongStr()
        {
            return GetGuidToLong().ToString();
        }

        /// <summary>
        ///     获取字符串型Ulong类型GUID
        /// </summary>
        /// <returns></returns>
        public static string ConvertGuidToLongStr(string guid)
        {
            return ConvertToLong(guid).ToString();
        }
    }
}
