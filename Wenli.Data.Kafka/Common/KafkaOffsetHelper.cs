/****************************************************************************
*项目名称：Wenli.Data.Kafka.Model
*CLR 版本：4.0.30319.42000
*机器名称：WALLE-PC
*命名空间：Wenli.Data.Kafka.Model
*类 名 称：KafkaOffset
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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Wenli.Data.Kafka.Common;

namespace Wenli.Data.Kafka.Model
{
    /// <summary>
    /// 本地持久化记录
    /// </summary>
    public static class KafkaOffsetHelper
    {

        static readonly string _path;


        static List<TopicPartitionOffset> _kafkaOffsets;


        static SemaphoreSlim _semaphoreSlim;

        static KafkaOffsetHelper()
        {
            _path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TopicPartitionOffset.json");

            _kafkaOffsets = new List<TopicPartitionOffset>();

            _semaphoreSlim = new SemaphoreSlim(1);
        }

        public static List<TopicPartitionOffset> Load()
        {
            List<TopicPartitionOffset> result = null;
            _semaphoreSlim.Wait();
            try
            {
                using (var fs = File.Open(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
                {
                    using (StreamReader sr = new StreamReader(fs))
                    {
                        var json = sr.ReadToEnd();

                        if (!string.IsNullOrWhiteSpace(json))
                        {
                            var data = SerializeUtil.Deserialize<List<TopicPartitionOffset>>(json);

                            if (data != null && data.Any())
                            {
                                _kafkaOffsets = data;
                                result = _kafkaOffsets.ToList();
                            }
                        }
                    }
                }
            }
            catch { }
            _semaphoreSlim.Release();
            return result;
        }

        public static async Task Save()
        {
            _semaphoreSlim.Wait();
            try
            {
                using (var fs = File.Open(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
                {
                    using (StreamWriter sw = new StreamWriter(fs))
                    {
                        var json = SerializeUtil.Serialize(_kafkaOffsets);

                        await sw.WriteAsync(json);
                    }
                }
            }
            catch { }
            _semaphoreSlim.Release();
        }

        public static async void Set(TopicPartitionOffset kafkaOffset)
        {
            _semaphoreSlim.Wait();
            try
            {
                if (_kafkaOffsets == null)
                {
                    _kafkaOffsets = new List<TopicPartitionOffset>();
                }

                var old = _kafkaOffsets.Where(b => b.Topic == kafkaOffset.Topic && b.Partition == kafkaOffset.Partition).FirstOrDefault();

                if (old != null)
                {
                    _kafkaOffsets.Remove(old);
                }
                _kafkaOffsets.Add(kafkaOffset);
            }
            catch { }
            _semaphoreSlim.Release();

            await Save();
        }



    }



}
