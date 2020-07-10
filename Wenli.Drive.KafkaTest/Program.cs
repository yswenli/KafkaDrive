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
using Wenli.Data.Kafka;
using Wenli.Data.Kafka.Common;
using Wenli.Drive.Kafka;

namespace Wenli.Drive.KafkaTest
{
    class Program
    {
        static readonly string server = "127.0.0.1:9092";

        static readonly string topic = "Wenli.Data.Kafka.Topic.Test";

        static readonly string group = "Wenli.Data.Kafka.Group.Test";

        public static void Main(params string[] args)
        {
            Console.Title = "Wenli.Data.Kafka";

            Console.WriteLine("Wenli.Data.Kafka");

            do
            {
                Console.WriteLine("输入p启动producer，输入c1启动consumer1，输入c2启动consumer2，输入a或其他启动全部！");

                var input = Console.ReadLine();

                switch (input)
                {
                    case "p":
                        _ = InitProducer();
                        Console.WriteLine("Wenli.Data.Kafka正在运行 producer");
                        break;

                    case "c1":
                        InitConsumer1();
                        Console.WriteLine("Wenli.Data.Kafka正在运行 consumer");
                        break;

                    case "c2":
                        InitConsumer2();
                        Console.WriteLine("Wenli.Data.Kafka正在运行 consumer");
                        break;

                    default:
                        _ = InitProducer();
                        Console.WriteLine("Wenli.Data.Kafka正在运行 producer");
                        InitConsumer1();
                        InitConsumer2();
                        Console.WriteLine("Wenli.Data.Kafka正在运行 consumer");
                        break;
                }

                Console.ReadLine();
            }
            while (true);
        }

        static async Task InitProducer()
        {
            var count = 0;

            Console.WriteLine("输入发送的消息数，默认为3条");
            var input = Console.ReadLine();

            if (!int.TryParse(input, out count))
            {
                count = 3;
            }

            var kafkaProducer = new KafkaProducer(server);

            await Task.Run(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var msg = SerializeUtil.Serialize(new TestData() { ID = GuidUtil.GuidString, Message = "Wenli.Data.Kafka.Test", Created = DateTimeUtil.CurrentDateTimeString });
                        kafkaProducer.SendMessage(topic, msg);
                        Console.WriteLine($"KafkaProducer.SendMessage:{msg}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"KafkaProducer.SendMessage Error:{ex.Message}");
                    }

                    Thread.Sleep(100);
                }
            }).ConfigureAwait(true);
        }

        static void InitConsumer1()
        {
            KafkaConsumer consumer = new KafkaConsumer(server, new string[] { topic }, group);
            consumer.OnReceived += Consumer_OnReceived;
            consumer.OnError += Consumer_OnError;
            consumer.Start();
        }
        static void InitConsumer2()
        {
            KafkaConsumer consumer = new KafkaConsumer(server, new string[] { topic }, group, true, false);
            consumer.OnReceived += Consumer_OnReceived;
            consumer.OnError += Consumer_OnError;
            consumer.Start();
        }



        private static void Consumer_OnReceived(KafkaConsumer arg1, KafkaMessage arg2)
        {
            Console.WriteLine($"KafkaConsumer.Receive:{SerializeUtil.Serialize(arg2)}");
        }

        private static void Consumer_OnError(KafkaConsumer arg1, Exception arg2)
        {
            Console.WriteLine($"KafkaConsumer.Error:{arg2}");
        }
    }

    #region TestData
    public class TestData
    {
        public string ID { get; set; }

        public string Message { get; set; }

        public string Created { get; set; }
    }
    #endregion
}
