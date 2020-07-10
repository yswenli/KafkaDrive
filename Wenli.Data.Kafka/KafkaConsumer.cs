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
using Wenli.Data.Kafka.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// kafka消费者
    /// </summary>
    public class KafkaConsumer : IDisposable
    {
        private bool _isStoped;
        private IConsumer<string, string> _consumer;
        private Task _consumerTask = null;
        /// <summary>
        /// 是否自动记录上次位置，
        /// 要使用最新记录+自动commit才生效
        /// </summary>
        private bool _autoCommitRecord = false;

        /// <summary>
        /// kafka消费者
        /// </summary>
        /// <param name="servers"></param>
        /// <param name="topics"></param>
        /// <param name="groupID"></param>
        /// <param name="autoCommit"></param>
        /// <param name="fromQueueHead"></param>
        /// <param name="logger"></param>
        public KafkaConsumer(string servers, string[] topics, string groupID, bool autoCommit = true, bool fromQueueHead = true)
        {
            _isStoped = false;

            Dictionary<string, string> dic = new Dictionary<string, string>()
            {
                { "bootstrap.servers", servers },
                { "group.id", groupID },
                { "log.connection.close", "false" }
            };

            dic.Add("enable.auto.commit", autoCommit.ToString().ToLower());

            //earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
            //latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
            //提交过offset，latest和earliest没有区别，但是在没有提交offset情况下，用latest直接会导致无法读取旧数据。
            //默认是latest，此处为了不丢数据改为 earliest
            if (fromQueueHead)
            {
                dic.Add("auto.offset.reset", "earliest");
            }
            else
            {
                dic.Add("auto.offset.reset", "latest");
            }

            var consumerConfig = new ConsumerConfig(dic);

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

            //为了避免latest 在autocommit下丢数据，进一步加强
            if ("latest".Equals(dic["auto.offset.reset"]?.ToString()) && autoCommit)
            {
                _autoCommitRecord = true;

                var topicPartitionOffsets = KafkaOffsetHelper.Load();

                if (topicPartitionOffsets != null && topicPartitionOffsets.Any())
                {
                    _consumer.Assign(topicPartitionOffsets);
                }
            }
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

        public void Start()
        {
            _consumerTask = Task.Factory.StartNew(() =>
             {
                 while (!_isStoped)
                 {
                     try
                     {
                         Message<string, string> message = null;

                         var consumerResult = _consumer.Consume(TimeSpan.FromSeconds(10));

                         if (consumerResult != null)
                         {
                             var kafkaMessage = new KafkaMessage(consumerResult.Topic, consumerResult.Partition, consumerResult.Offset.Value, consumerResult.Message.Key, consumerResult.Message.Value, consumerResult.Message.Timestamp.UnixTimestampMs);

                             try
                             {
                                 OnReceived?.Invoke(this, kafkaMessage);
                             }
                             catch { }

                             if (_autoCommitRecord)
                             {
                                 KafkaOffsetHelper.Set(new TopicPartitionOffset(kafkaMessage.Topic, kafkaMessage.Partition, kafkaMessage.Offset));
                                 _ = KafkaOffsetHelper.Save();
                             }

                         }
                     }
                     catch (Exception exception)
                     {
                         OnError?.Invoke(this, exception);
                     }
                 }

             }, TaskCreationOptions.LongRunning);
        }

        public void Commit(KafkaMessage message)
        {
            try
            {

                _consumer?.Commit(new ConsumeResult<string, string>()
                {
                    Topic = message.Topic,
                    Partition = message.Partition,
                    Offset = message.Offset,
                    Message = new Message<string, string>()
                    {
                        Key = message.Key,
                        Value = message.Value,
                        Timestamp = new Timestamp(message.UnixCreatedTimeMS, TimestampType.CreateTime)
                    }
                });
            }
            catch (Exception exception)
            {
                OnError?.Invoke(this, exception);
            }
        }

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
                OnError?.Invoke(this, ex);
            }
            finally
            {
                _consumer = null;
            }
        }

        /// <summary>
        /// 收到消息
        /// </summary>
        public event Action<KafkaConsumer, KafkaMessage> OnReceived;

        public event Action<KafkaConsumer, Exception> OnError;

        public void Dispose()
        {
            this.Stop();
        }
    }
}
