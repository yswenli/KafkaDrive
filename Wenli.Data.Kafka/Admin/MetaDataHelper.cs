/****************************************************************************
*项目名称：Wenli.Data.Kafka.Admin
*CLR 版本：4.0.30319.42000
*机器名称：WALLE-PC
*命名空间：Wenli.Data.Kafka.Admin
*类 名 称：MetaDataHelper
*版 本 号：V1.0.0.0
*创建人： yswenli
*电子邮箱：yswenli@outlook.com
*创建时间：2020/10/31 16:15:21
*描述：
*=====================================================================
*修改时间：2020/10/31 16:15:21
*修 改 人： yswenli
*版 本 号： V1.0.0.0
*描    述：
*****************************************************************************/
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;

namespace Wenli.Data.Kafka.Admin
{
    /// <summary>
    /// MetaDataHelper
    /// </summary>
    public class MetaDataHelper
    {
        Producer<byte[], byte[]> _producer;

        Consumer<byte[], byte[]> _consumer;

        string _kafkaUrl;

        /// <summary>
        /// MetaDataHelper
        /// </summary>
        /// <param name="consumer"></param>
        internal MetaDataHelper(string kafkaUrl)
        {
            _kafkaUrl = kafkaUrl;
            Dictionary<string, object> configuration = new Dictionary<string, object>()
            {
                { "bootstrap.servers", kafkaUrl },
                { "log.connection.close", false }
            };
            _producer = new Producer<byte[], byte[]>(configuration, new ByteArraySerializer(), new ByteArraySerializer());
        }
        /// <summary>
        /// GetMetadata
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Metadata GetMetadata(int timeout = 10)
        {
            return _producer.GetMetadata(true, "", TimeSpan.FromSeconds(timeout));
        }
        /// <summary>
        /// GetBrokerMetadatas
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<BrokerMetadata> GetBrokerMetadatas(int timeout = 10)
        {
            return GetMetadata(timeout).Brokers;
        }
        /// <summary>
        /// GetTopicMetadatas
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<TopicMetadata> GetTopicMetadatas(int timeout = 10)
        {
            return GetMetadata(timeout).Topics;
        }


        /// <summary>
        /// GetGroupInfos
        /// </summary>
        /// <param name="topics"></param>
        /// <param name="groupID"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<GroupInfo> GetGroupInfos(string topics, string groupID = "_kafkatoolgroup", int timeout = 10)
        {
            if (_consumer == null)
            {
                Dictionary<string, object> configuration = new Dictionary<string, object>()
                {
                    { "bootstrap.servers", _kafkaUrl },
                    { "group.id", groupID },
                    { "log.connection.close", "false" },
                    { "enable.auto.commit","true"},
                    { "auto.offset.reset","latest"}
                };

                _consumer = new Consumer<byte[], byte[]>(configuration, new ByteArrayDeserializer(), new ByteArrayDeserializer());

                _consumer.Subscribe(topics);
            }

            return _consumer.ListGroups(TimeSpan.FromSeconds(timeout));
        }

        /// <summary>
        /// Create
        /// </summary>
        /// <param name="kafkaUrl"></param>
        /// <param name="topics"></param>
        /// <param name="groupID"></param>
        /// <returns></returns>
        public static MetaDataHelper Create(string kafkaUrl, string topics = "_kafkatooltopic", string groupID = "_kafkatoolgroup")
        {
            return new MetaDataHelper(kafkaUrl);
        }
    }
}
