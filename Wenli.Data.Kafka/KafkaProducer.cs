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
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace Wenli.Data.Kafka
{
    /// <summary>
    /// kafka生产者
    /// </summary>
    public class KafkaProducer : IDisposable
    {
        private IProducer<string, string> _producer;

        string _servers = string.Empty;

        /// <summary>
        /// kafka生产者
        /// </summary>
        /// <param name="servers"></param>
        /// <param name="msgTimeoutMs"></param>
        public KafkaProducer(string servers, int msgTimeoutMs = 5 * 1000)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = servers,
                MessageTimeoutMs = msgTimeoutMs
            };
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
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
            try
            {
                var deliveryResult = await _producer.ProduceAsync(topic, new Message<string, string>() { Key = key, Value = message });

                if (deliveryResult != null)
                {
                    return new KafkaMessage(deliveryResult.Topic, deliveryResult.Partition, deliveryResult.Offset, deliveryResult.Key, deliveryResult.Value, deliveryResult.Timestamp.UnixTimestampMs);
                }
            }
            catch (Exception ex)
            {
                OnError?.Invoke(this, new Exception($"{ex.Message},params:{_servers},{topic},{key}, {message}, {partitionId}"));
            }

            return null;
        }

        /// <summary>
        /// 同步发送数据，
        /// 同步的时候，不要再在外面套用Task
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="message"></param>
        /// <param name="partitionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public KafkaMessage SendMessage(string topic, string message, int partitionId = -1, string key = "")
        {
            var task = SendMessageAsync(topic, message, partitionId, key);

            task.ConfigureAwait(false);

            if (task.Wait(5 * 1000))
            {
                return task.Result;
            }

            return null;
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
        public event Action<KafkaProducer, Exception> OnError;

        private void Producer_OnError(object sender, Error e)
        {
            OnError?.Invoke(this, new Exception(JsonConvert.SerializeObject(e)));
        }
    }
}