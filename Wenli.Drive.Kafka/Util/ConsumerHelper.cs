/*****************************************************************************************************
 * 本代码版权归@wenli所有，All Rights Reserved (C) 2015-2017
 *****************************************************************************************************
 * CLR版本：4.0.30319.42000
 * 唯一标识：37b509d9-2fe4-4e5e-b1aa-080d67179289
 * 机器名称：WENLI-PC
 * 联系人邮箱：wenguoli_520@qq.com
 *****************************************************************************************************
 * 项目名称：$projectname$
 * 命名空间：Wenli.Drive.Kafka.Util
 * 类名称：ConsumerHelper
 * 创建时间：2017/2/10 17:00:46
 * 创建人：wenli
 * 创建说明：
 *****************************************************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace Wenli.Drive.Kafka.Util
{
    /// <summary>
    /// 消费者辅助类
    /// </summary>
    internal class ConsumerHelper
    {
        private BrokerHelper _brokerHelper;

        private Consumer _consumer;

        private bool _unSub = false;

        public ConsumerHelper(BrokerHelper brokerHelper)
        {
            _brokerHelper = brokerHelper;
        }

        public void Sub(string topic, Action<string> onMsg)
        {
            _unSub = false;

            var opiton = new ConsumerOptions(topic, _brokerHelper.GetBroker());

            _consumer = new Consumer(opiton);

            Task.Run(() =>
            {
                while (!_unSub)
                {
                    var msgs = _consumer.Consume();
                    Parallel.ForEach(msgs, msg =>
                    {
                        onMsg(Encoding.UTF8.GetString(msg.Value));
                    });
                }
            });
        }


        public void UnSub()
        {
            _unSub = true;
        }



    }
}
