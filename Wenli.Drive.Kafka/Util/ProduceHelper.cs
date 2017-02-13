/*****************************************************************************************************
 * 本代码版权归@wenli所有，All Rights Reserved (C) 2015-2017
 *****************************************************************************************************
 * CLR版本：4.0.30319.42000
 * 唯一标识：388bd3c2-e901-49cd-8126-aef0e0e8274d
 * 机器名称：WENLI-PC
 * 联系人邮箱：wenguoli_520@qq.com
 *****************************************************************************************************
 * 项目名称：$projectname$
 * 命名空间：Wenli.Drive.Kafka.Util
 * 类名称：ProduceHelper
 * 创建时间：2017/2/10 17:00:11
 * 创建人：wenli
 * 创建说明：
 *****************************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Protocol;

namespace Wenli.Drive.Kafka.Util
{
    /// <summary>
    /// 生产者辅助类
    /// </summary>
    internal class ProduceHelper : IDisposable
    {
        private BrokerHelper _brokerHelper;

        private Producer _producer;

        public ProduceHelper(BrokerHelper brokerHelper)
        {
            _brokerHelper = brokerHelper;

            _producer = new Producer(_brokerHelper.GetBroker());
        }

        /// <summary>
        /// 发送消息到队列
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="datas"></param>
        /// <param name="acks"></param>
        /// <param name="timeout"></param>
        /// <param name="codec"></param>
        public void Pub(string topic, List<string> datas, short acks = 1, TimeSpan? timeout = default(TimeSpan?), MessageCodec codec = MessageCodec.CodecNone)
        {
            var msgs = new List<Message>();
            foreach (var item in datas)
            {
                msgs.Add(new Message(item));
            }
            _producer.SendMessageAsync(topic, msgs, acks, timeout, codec);
        }


        public void Dispose()
        {
            if (_producer != null)
                _producer.Dispose();
        }
    }
}
