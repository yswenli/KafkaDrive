using System.Text;

namespace Wenli.Data.Kafka
{
    public class KafkaMessage : KafkaMessageBase
    {
        public KafkaMessage(string topic, int partition, long offset, string key, string value, long unixCreatedTimeMS)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = value;
            UnixCreatedTimeMS = unixCreatedTimeMS;
        }

        public new string Key
        {
            get;
            set;
        }

        public new string Value
        {
            get;
            set;
        }
    }

    public class KafkaMessageBase
    {
        protected KafkaMessageBase() { }

        public KafkaMessageBase(string topic, int partition, long offset, byte[] key, byte[] value, long unixCreatedTimeMS)
        {
            Topic = topic;
            Partition = partition;
            Offset = offset;
            Key = key;
            Value = value;
            UnixCreatedTimeMS = unixCreatedTimeMS;
        }

        public string Topic { get; set; }

        public int Partition { get; set; }

        public long Offset { get; set; }

        public byte[] Key { get; set; }

        public byte[] Value { get; set; }

        public long UnixCreatedTimeMS { get; set; }

    }

    /// <summary>
    /// ModelExtention
    /// </summary>
    public static class ModelExtention
    {
        /// <summary>
        /// ToKafkaMessage
        /// </summary>
        /// <returns></returns>
        public static KafkaMessage ToKafkaMessage(this KafkaMessageBase kafkaMessageBase)
        {
            return new KafkaMessage(kafkaMessageBase.Topic, kafkaMessageBase.Partition, kafkaMessageBase.Offset,
                kafkaMessageBase.Key == null ? string.Empty : Encoding.UTF8.GetString(kafkaMessageBase.Key),
                kafkaMessageBase.Value == null ? string.Empty : Encoding.UTF8.GetString(kafkaMessageBase.Value), kafkaMessageBase.UnixCreatedTimeMS);
        }
        /// <summary>
        /// ToKafkaMessageBase
        /// </summary>
        /// <param name="kafkaMessage"></param>
        /// <returns></returns>
        public static KafkaMessageBase ToKafkaMessageBase(this KafkaMessage kafkaMessage)
        {
            return new KafkaMessageBase(kafkaMessage.Topic, kafkaMessage.Partition, kafkaMessage.Offset,
                string.IsNullOrEmpty(kafkaMessage.Key) ? null : Encoding.UTF8.GetBytes(kafkaMessage.Key),
                string.IsNullOrEmpty(kafkaMessage.Value) ? null : Encoding.UTF8.GetBytes(kafkaMessage.Value),
                kafkaMessage.UnixCreatedTimeMS);
        }
    }
}
