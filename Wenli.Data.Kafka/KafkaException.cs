/****************************************************************************
*项目名称：Wenli.Data.Kafka
*CLR 版本：4.0.30319.42000
*机器名称：WALLE-PC
*命名空间：Wenli.Data.Kafka
*类 名 称：KafkaException
*版 本 号：V1.0.0.0
*创建人： yswenli
*电子邮箱：yswenli@outlook.com
*创建时间：2020/10/29 14:46:47
*描述：
*=====================================================================
*修改时间：2020/10/29 14:46:47
*修 改 人： yswenli
*版 本 号： V1.0.0.0
*描    述：
*****************************************************************************/
using System;
using Wenli.Data.Kafka.Common;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// KafkaException
    /// </summary>
    public class KafkaException : Exception
    {
        /// <summary>
        /// KafkaException
        /// </summary>
        /// <param name="des"></param>
        /// <param name="ex"></param>
        /// <param name="objs"></param>
        public KafkaException(string des, Exception ex, params object[] objs) : this($"{des},error:{SerializeUtil.Serialize(ex)} ,params:{(objs == null ? "" : SerializeUtil.Serialize(objs))}", ex)
        {

        }

        /// <summary>
        /// KafkaException
        /// </summary>
        /// <param name="msg"></param>
        public KafkaException(string msg) : base(msg)
        {

        }
        /// <summary>
        /// KafkaException
        /// </summary>
        /// <param name="msg"></param>
        /// <param name="ex"></param>
        public KafkaException(string msg, Exception ex) : base(msg, ex)
        {

        }
    }
}
