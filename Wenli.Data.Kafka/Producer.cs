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
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Wenli.Data.Kafka.Common;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// kafka生产者
    /// </summary>
    public class Producer : IDisposable
    {
        ProducerBase _producerBase;

        /// <summary>
        /// Configuration
        /// </summary>
        public Dictionary<string, object> Configuration
        {
            get; private set;
        }

        /// <summary>
        /// kafka生产者
        /// </summary>
        /// <param name="servers"></param>
        public Producer(string servers)
        {
            _producerBase = new ProducerBase(servers);
            Configuration = _producerBase.Configuration;
            _producerBase.OnError += _producerBase_OnError;
        }

        /// <summary>
        /// 异常事件
        /// </summary>
        public event Action<Producer, KafkaException> OnError;

        private void _producerBase_OnError(ProducerBase arg1, KafkaException arg2)
        {
            OnError?.Invoke(this, arg2);
        }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="partitionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<KafkaMessage> SendMessageAsync(string topic, string message, int partitionId = -1, string key = "")
        {
            if (string.IsNullOrEmpty(message)) return null;

            var km = await _producerBase.SendMessageAsync(topic, Encoding.UTF8.GetBytes(message), partitionId, (string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key)));

            return km.ToKafkaMessage();
        }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="partitionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<KafkaMessage> SendMessageAsync<T>(string topic, T message, int partitionId = -1, string key = "") where T : new()
        {
            return await SendMessageAsync(topic, SerializeUtil.Serialize(message), partitionId, key);
        }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<KafkaMessage> SendMessageAsyncForBalance(string topic, string message, string key)
        {
            if (string.IsNullOrEmpty(message)) return null;

            var km = await _producerBase.SendMessageAsyncForBalance(topic, Encoding.UTF8.GetBytes(message), (string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key)));

            return km.ToKafkaMessage();
        }

        /// <summary>
        /// 发送消息
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<KafkaMessage> SendMessageAsyncForBalance<T>(string topic, T message, string key) where T : new()
        {
            return await SendMessageAsyncForBalance(topic, SerializeUtil.Serialize(message), key);
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
        public KafkaMessage SendMessage(string topic, string message, int partitionId = -1, string key = "")
        {
            if (string.IsNullOrEmpty(message)) return null;

            var km = _producerBase.SendMessage(topic, Encoding.UTF8.GetBytes(message), partitionId, (string.IsNullOrEmpty(key) ? null : Encoding.UTF8.GetBytes(key)));

            return km.ToKafkaMessage();
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
        public KafkaMessage SendMessage<T>(string topic, T message, int partitionId = -1, string key = "") where T : new()
        {
            return SendMessage(topic, SerializeUtil.Serialize(message), partitionId, key);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            _producerBase?.Dispose();
        }
    }
}