/****************************************************************************
*项目名称：Wenli.Data.Kafka.Model
*CLR 版本：4.0.30319.42000
*机器名称：WALLE-PC
*命名空间：Wenli.Data.Kafka
*类 名 称：KafkaConsumer
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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Wenli.Data.Kafka.Common;
using Wenli.Data.Kafka.Model;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// kafka消费者
    /// </summary>
    public class ConsumerBase : IDisposable
    {
        private bool _isStoped;

        bool _fromConfig = false;

        private Consumer<byte[], byte[]> _consumer;

        private Task _consumerTask = null;

        /// <summary>
        /// 是否自动记录上次位置，
        /// 要使用最新记录+自动commit才生效
        /// </summary>
        private bool _autoCommitRecord = false;

        /// <summary>
        /// 收到消息
        /// </summary>
        public event Action<ConsumerBase, KafkaMessageBase> OnReceived;
        /// <summary>
        /// 异常消息
        /// </summary>
        public event Action<ConsumerBase, KafkaException> OnError;

        /// <summary>
        /// Configuration
        /// </summary>
        public Dictionary<string, object> Configuration
        {
            get; private set;
        }

        /// <summary>
        /// kafka消费者
        /// </summary>
        /// <param name="servers"></param>
        /// <param name="topics"></param>
        /// <param name="groupID"></param>
        /// <param name="autoCommit"></param>
        /// <param name="fromQueueHead"></param>
        /// <param name="fromConfig"></param>
        public ConsumerBase(string servers, IEnumerable<string> topics, string groupID,
            bool autoCommit = true, bool fromQueueHead = true, bool fromConfig = false)
        {
            _isStoped = false;

            _fromConfig = fromConfig;

            Configuration = new Dictionary<string, object>()
            {
                { "bootstrap.servers", servers },
                { "group.id", groupID },
                { "log.connection.close", "false" }
            };

            Configuration.Add("enable.auto.commit", autoCommit.ToString().ToLower());

            //earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
            //latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
            //提交过offset，latest和earliest没有区别，但是在没有提交offset情况下，用latest直接会导致无法读取旧数据。
            //默认是latest，此处为了不丢数据改为 earliest
            if (fromQueueHead)
            {
                Configuration.Add("auto.offset.reset", "earliest");
            }
            else
            {
                Configuration.Add("auto.offset.reset", "latest");
            }

            _consumer = new Consumer<byte[], byte[]>(Configuration, new ByteArrayDeserializer(), new ByteArrayDeserializer());

            if (_fromConfig)
            {
                //为了避免latest 在autocommit下丢数据，进一步加强
                if ("latest".Equals(Configuration["auto.offset.reset"]?.ToString()) && autoCommit)
                {
                    _autoCommitRecord = true;

                    var topicPartitionOffsets = KafkaOffsetHelper.Load();

                    if (topicPartitionOffsets != null && topicPartitionOffsets.Any())
                    {
                        _consumer.Assign(topicPartitionOffsets);
                    }
                }
            }

            _consumer.OnConsumeError += Consumer_OnConsumeError;
            _consumer.OnError += Consumer_OnError;
            _consumer.Subscribe(topics);
        }

        /// <summary>
        /// 重新订阅topics
        /// </summary>
        /// <param name="topics"></param>
        public void ReSubscribe(string[] topics)
        {
            _consumer.Unassign();
            _consumer.Unsubscribe();
            _consumer.Subscribe(topics);
        }
        /// <summary>
        /// Start
        /// </summary>
        public void Start()
        {
            _consumerTask = Task.Factory.StartNew(() =>
            {
                 while (!_isStoped)
                 {
                     try
                     {
                         Message<byte[], byte[]> message;

                         if (_consumer.Consume(out message, TimeSpan.FromSeconds(5)))
                         {
                             var kafkaMessageBase = new KafkaMessageBase(message.Topic, message.Partition, message.Offset.Value, message.Key, message.Value, message.Timestamp.UnixTimestampMs);
                             try
                             {
                                 OnReceived?.Invoke(this, kafkaMessageBase);
                             }
                             catch (Exception ex)
                             {
                                 OnError?.Invoke(this, new KafkaException("Consumer Start异常", ex, message));
                             }

                             if (_autoCommitRecord && _fromConfig)
                             {
                                 KafkaOffsetHelper.Set(new TopicPartitionOffset(message.Topic, message.Partition, message.Offset.Value));
                                 _ = KafkaOffsetHelper.Save();
                             }

                         }
                     }
                     catch (Exception ex)
                     {
                         OnError?.Invoke(this, new KafkaException("Consumer Start异常", ex, _consumer));
                     }
                 }

             }, TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Commit
        /// </summary>
        /// <param name="message"></param>
        public void Commit(KafkaMessageBase message)
        {
            try
            {
                _consumer?.CommitAsync(new Message<byte[], byte[]>(message.Topic, message.Partition, message.Offset, message.Key, message.Value, new Timestamp(message.UnixCreatedTimeMS, TimestampType.NotAvailable), null));

                if (_fromConfig)
                {
                    KafkaOffsetHelper.Set(new TopicPartitionOffset(message.Topic, message.Partition, message.Offset));
                    _ = KafkaOffsetHelper.Save();
                }
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, new KafkaException("Consumer Commit异常", ex, message));
            }
        }
        /// <summary>
        /// Stop
        /// </summary>
        public void Stop()
        {
            try
            {
                _isStoped = true;
                _consumerTask?.Wait(3000);
                _consumer?.Dispose();
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, new KafkaException("Consumer Stop异常", ex, _consumer));
            }
            finally
            {
                _consumer = null;
            }
        }



        private void Consumer_OnError(object sender, Error e)
        {
            OnError?.Invoke(this, new KafkaException("Consumer 异常", new Exception(SerializeUtil.Serialize(e)), _consumer));
        }

        private void Consumer_OnConsumeError(object sender, Message e)
        {
            OnError?.Invoke(this, new KafkaException("Consumer 异常", new Exception(SerializeUtil.Serialize(e)), _consumer));
        }
        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            this.Stop();
        }
    }
}
