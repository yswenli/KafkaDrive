/****************************************************************************
*项目名称：Wenli.Data.Kafka.Model
*CLR 版本：4.0.30319.42000
*机器名称：WALLE-PC
*命名空间：Wenli.Data.Kafka
*类 名 称：KafkaProducer
*版 本 号：V1.0.0.0
*创建人： yswenli
*电子邮箱：yswenli@outlook.com
*创建时间：2019/12/9 17:45:20
*描述：
*=====================================================================
*修改时间：2019/12/9 17:45:20
*修 改 人： yswenli
*版 本 号： V1.0.0.0
*描    述：
*****************************************************************************/
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wenli.Data.Kafka.Common;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// kafka生产者
    /// </summary>
    public class ProducerBase : IDisposable
    {
        private Producer<byte[], byte[]> _producer;

        Metadata _metadata = null;

        ConcurrentDictionary<string, List<int>> _topicPartitionIds;

        /// <summary>
        /// Configuration
        /// </summary>
        public Dictionary<string, object> Configuration
        {
            get;private set;
        }

        /// <summary>
        /// kafka生产者
        /// </summary>
        /// <param name="servers"></param>
        /// <param name="logger"></param>
        public ProducerBase(string servers)
        {
            Configuration = new Dictionary<string, object>()
            {
                { "bootstrap.servers", servers },
                { "log.connection.close", false }
            };

            _producer = new Producer<byte[], byte[]>(Configuration, new ByteArraySerializer(), new ByteArraySerializer());

            _producer.OnError += Producer_OnError;

            _topicPartitionIds = new ConcurrentDictionary<string, List<int>>();
        }

        /// <summary>
        /// 获取运算分区
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        private int GetPartitionId(string topic, int val)
        {
            if (val == -1) return -1;

            var pc = 8;

            var ps = GetPartitionIds(topic);

            if (ps != null && ps.Count > 0)
            {
                pc = ps.Count;
            }

            return Math.Abs(val % pc);
        }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="partitionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<KafkaMessageBase> SendMessageAsync(string topic, byte[] message, int partitionId = -1, byte[] key = null)
        {
            try
            {
                if (message == null) return null;

                partitionId = GetPartitionId(topic, partitionId);

                var result = await _producer?.ProduceAsync(topic, key, message, partitionId);

                return new KafkaMessageBase(result.Topic, result.Partition, result.Offset, result.Key, result.Value, result.Timestamp.UnixTimestampMs);
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, new KafkaException("Producer 发送数据异常", ex, topic, partitionId));
            }
            return null;
        }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="key"></param>
        public async Task<KafkaMessageBase> SendMessageAsyncForBalance(string topic, byte[] message, byte[] key)
        {
            try
            {
                if (message == null) return null;

                var partitionIds = GetPartitionIds(topic);

                if (partitionIds == null || !partitionIds.Any())
                {
                    var result = await _producer?.ProduceAsync(topic, key, message, -1);

                    return new KafkaMessageBase(result.Topic, result.Partition, result.Offset, result.Key, result.Value, result.Timestamp.UnixTimestampMs);
                }
                else
                {
                    var pid = Math.Abs(message.GetHashCode()) % partitionIds.Count;

                    var result = await _producer?.ProduceAsync(topic, key, message, partitionIds[pid]);

                    return new KafkaMessageBase(result.Topic, result.Partition, result.Offset, result.Key, result.Value, result.Timestamp.UnixTimestampMs);
                }
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, new KafkaException("Producer 发送数据异常", ex, topic));
            }
            return null;
        }

        /// <summary>
        /// 同步发送数据，
        /// 同步的时候，不要再在外面套用Task，否则后果自负
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="partitionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public KafkaMessageBase SendMessage(string topic, byte[] message, int partitionId = -1, byte[] key = null)
        {
            KafkaMessageBase msg = null;
            try
            {
                if (message == null) return null;

                msg = SendMessageAsync(topic, message, partitionId, key).Result;
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, new KafkaException("Producer 发送数据异常", ex, topic, partitionId));
            }
            return msg;
        }

        /// <summary>
        /// 获取分区集合
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public List<int> GetPartitionIds(string topic)
        {
            return _topicPartitionIds.GetOrAdd(topic, (k) =>
            {
                if (_metadata == null)
                {
                    _metadata = _producer.GetMetadata(true, string.Empty);
                }

                if (_metadata != null && _metadata.Topics != null && _metadata.Topics.Any())
                {
                    var tinfo = _metadata.Topics.Where(b => b.Topic == topic).FirstOrDefault();

                    if (tinfo != null && tinfo.Partitions != null && tinfo.Partitions.Any())
                    {
                        return tinfo.Partitions.Select(b => b.PartitionId).ToList();
                    }
                }
                return null;
            });
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            try
            {
                _producer?.Dispose();
            }
            catch
            {
                _producer = null;
            }
        }

        /// <summary>
        /// 异常事件
        /// </summary>
        public event Action<ProducerBase, KafkaException> OnError;

        private void Producer_OnError(object sender, Error e)
        {
            OnError?.Invoke(this, new KafkaException("Producer Error", new Exception(SerializeUtil.Serialize(e)), sender));
        }
    }
}