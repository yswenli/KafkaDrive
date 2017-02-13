/*****************************************************************************************************
 * 本代码版权归@wenli所有，All Rights Reserved (C) 2015-2017
 *****************************************************************************************************
 * CLR版本：4.0.30319.42000
 * 唯一标识：1e927a20-9d60-4da9-9dce-cf5940da3594
 * 机器名称：WENLI-PC
 * 联系人邮箱：wenguoli_520@qq.com
 *****************************************************************************************************
 * 项目名称：$projectname$
 * 命名空间：Wenli.Drive.Kafka
 * 类名称：KafkaHelper
 * 创建时间：2017/2/10 16:52:29
 * 创建人：wenli
 * 创建说明：
 *****************************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Wenli.Drive.Kafka.Util;

namespace Wenli.Drive.Kafka
{
    /// <summary>
    /// kafka辅助类
    /// </summary>
    public sealed class KafkaHelper
    {
        private bool _isProducer = true;

        private KafkaConfig _config;

        private BrokerHelper _brokerHelper;

        private ProduceHelper _produceHelper;

        private ConsumerHelper _consumerHelper;

        /// <summary>
        /// 是否是生产者模式
        /// </summary>
        public bool IsProducer
        {
            get
            {
                return _isProducer;
            }
        }
        /// <summary>
        /// kafka辅助类构造方法
        /// </summary>
        /// <param name="sectionName">config中配置节点名称</param>
        /// <param name="isProducer"></param>
        public KafkaHelper(string sectionName, bool isProducer = true)
        {
            _isProducer = isProducer;
            _config = KafkaConfig.GetConfig(sectionName);
            _brokerHelper = new BrokerHelper(_config.Broker);
            if (isProducer)
                _produceHelper = new ProduceHelper(_brokerHelper);
            else
                _consumerHelper = new ConsumerHelper(_brokerHelper);
        }


        /// <summary>
        /// 发送消息到队列
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="datas"></param>
        /// <param name="acks"></param>
        /// <param name="timeout"></param>
        public void Pub(List<string> datas, short acks = 1, TimeSpan? timeout = default(TimeSpan?))
        {
            _produceHelper.Pub(_config.Topic, datas, acks, timeout, MessageCodec.CodecNone);
        }

        /// <summary>
        /// 订阅消息
        /// </summary>
        /// <param name="onMsg"></param>
        public void Sub( Action<string> onMsg)
        {
            _consumerHelper.Sub(_config.Topic, onMsg);
        }
        /// <summary>
        /// 取消订阅
        /// </summary>
        public void UnSub()
        {
            _consumerHelper.UnSub();
        }
    }
}
