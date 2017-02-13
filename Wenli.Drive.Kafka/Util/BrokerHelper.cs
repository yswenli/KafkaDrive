/*****************************************************************************************************
 * 本代码版权归@wenli所有，All Rights Reserved (C) 2015-2017
 *****************************************************************************************************
 * CLR版本：4.0.30319.42000
 * 唯一标识：77286ba3-14fb-4834-b865-77e690425d1d
 * 机器名称：WENLI-PC
 * 联系人邮箱：wenguoli_520@qq.com
 *****************************************************************************************************
 * 项目名称：$projectname$
 * 命名空间：Wenli.Drive.Kafka.Util
 * 类名称：BrokerHelper
 * 创建时间：2017/2/10 17:01:17
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

namespace Wenli.Drive.Kafka.Util
{
    /// <summary>
    /// 代理人辅助类
    /// </summary>
    internal class BrokerHelper
    {
        private string _broker;

        public BrokerHelper(string broker)
        {
            _broker = broker;
        }

        /// <summary>
        /// 获取代理的路由对象
        /// </summary>
        /// <returns></returns>
        public BrokerRouter GetBroker()
        {
            var options = new KafkaOptions(new Uri(string.Format("http://{0}", _broker)));
            return new BrokerRouter(options);
        }
    }
}
