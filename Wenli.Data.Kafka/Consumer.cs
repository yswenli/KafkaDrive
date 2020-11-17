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
using System;
using System.Collections.Generic;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// kafka消费者
    /// </summary>
    public class Consumer : IDisposable
    {

        ConsumerBase _consumerBase;

        /// <summary>
        /// 收到消息
        /// </summary>
        public event Action<Consumer, KafkaMessage> OnReceived;

        /// <summary>
        /// 异常消息
        /// </summary>
        public event Action<Consumer, Exception> OnError;


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
        public Consumer(string servers, IEnumerable<string> topics, string groupID, bool autoCommit = true,
            bool fromQueueHead = true, bool fromConfig = false)
        {
            _consumerBase = new ConsumerBase(servers, topics, groupID, autoCommit, fromQueueHead, fromConfig);

            Configuration = _consumerBase.Configuration;

            _consumerBase.OnError += _kafkaConsumerBase_OnError;

            _consumerBase.OnReceived += _kafkaConsumerBase_OnReceived;
        }

        private void _kafkaConsumerBase_OnReceived(ConsumerBase arg1, KafkaMessageBase arg2)
        {
            OnReceived?.Invoke(this, arg2.ToKafkaMessage());
        }

        private void _kafkaConsumerBase_OnError(ConsumerBase arg1, Exception arg2)
        {
            OnError?.Invoke(this, arg2);
        }

        /// <summary>
        /// 重新订阅topics
        /// </summary>
        /// <param name="topics"></param>
        public void ReSubscribe(string[] topics)
        {
            _consumerBase.ReSubscribe(topics);
        }
        /// <summary>
        /// Start
        /// </summary>
        public void Start()
        {
            _consumerBase.Start();
        }
        /// <summary>
        /// Commit
        /// </summary>
        /// <param name="message"></param>
        public void Commit(KafkaMessage message)
        {
            _consumerBase.Commit(message.ToKafkaMessageBase());
        }
        /// <summary>
        /// Stop
        /// </summary>
        public void Stop()
        {
            _consumerBase.Stop();
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
