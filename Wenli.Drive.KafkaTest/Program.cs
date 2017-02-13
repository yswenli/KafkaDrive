/*****************************************************************************************************
 * 本代码版权归@wenli所有，All Rights Reserved (C) 2015-2017
 *****************************************************************************************************
 * CLR版本：4.0.30319.42000
 * 唯一标识：37b509d9-2fe4-4e5e-b1aa-080d67179289
 * 机器名称：WENLI-PC
 * 联系人邮箱：wenguoli_520@qq.com
 *****************************************************************************************************
 * 命名空间：Wenli.Drive.Kafka.Util
 * 类名称：Program
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
using Wenli.Drive.Kafka;

namespace Wenli.Drive.KafkaTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string header = "wenli.drive.kafka测试";
            Console.Title = header;
            Console.WriteLine(header);
            var color = Console.ForegroundColor;

            var pub = new KafkaHelper("Test", true);

            var sub = new KafkaHelper("Test", false);

            Task.Run(() =>
            {
                while (true)
                {
                    
                    var msg = string.Format("{0}这是一条测试消息", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));                    
                    pub.Pub(new List<string>() { msg });

                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("发送消息：" + msg);
                    Console.ForegroundColor = color;
                    Thread.Sleep(500);
                }
            });

            Task.Run(() =>
            {
                sub.Sub((msg) =>
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine(string.Format("收到消息：{0}", msg));
                    Console.ForegroundColor = color;
                });
            });

            Console.ReadLine();
        }
    }
}
