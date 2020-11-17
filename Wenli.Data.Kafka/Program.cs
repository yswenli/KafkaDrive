/****************************************************************************
*项目名称：Wenli.Data.Kafka
*CLR 版本：4.0.30319.42000
*机器名称：WALLE-PC
*命名空间：Wenli.Data.Kafka
*类 名 称：Program
*版 本 号：V1.0.0.0
*创建人： yswenli
*电子邮箱：yswenli@outlook.com
*创建时间：2019/12/10 11:01:34
*描述：
*=====================================================================
*修改时间：2019/12/10 11:01:34
*修 改 人： yswenli
*版 本 号： V1.0.0.0
*描    述：
*****************************************************************************/
using System;
using System.Threading;
using System.Threading.Tasks;
using Wenli.Data.Kafka.Common;

namespace Wenli.Data.Kafka
{
    class Program
    {
        static readonly string server = "10.205.243.25:9092";

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

            var kafkaProducer = new Producer(server);

            await Task.Run(() =>
            {
                for (int i = 0; i < count; i++)
                {
                    try
                    {
                        var msg = SerializeUtil.Serialize(new TestData() { ID = GuidUtil.GuidString, Message = "Wenli.Data.Kafka.Test", Created = DateTimeUtil.CurrentDateTimeString });
                        if (i % 2 == 0)
                            kafkaProducer.SendMessage(topic, msg, -1, "aaa");
                        else
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
            Consumer consumer = new Consumer(server, new string[] { topic }, group);
            consumer.OnReceived += Consumer_OnReceived;
            consumer.OnError += Consumer_OnError;
            consumer.Start();
        }
        static void InitConsumer2()
        {
            Consumer consumer = new Consumer(server, new string[] { topic }, group, true, false);
            consumer.OnReceived += Consumer_OnReceived;
            consumer.OnError += Consumer_OnError;
            consumer.Start();
        }



        private static void Consumer_OnReceived(Consumer arg1, KafkaMessage arg2)
        {
            Console.WriteLine($"KafkaConsumer.Receive:{SerializeUtil.Serialize(arg2)}");
        }

        private static void Consumer_OnError(Consumer arg1, Exception arg2)
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
