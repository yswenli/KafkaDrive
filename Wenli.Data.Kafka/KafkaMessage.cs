namespace Wenli.Data.Kafka
{
    public class KafkaMessage
    {
        public KafkaMessage(string topic, int partition, long offset, string key, string value, long unixCreatedTimeMS)
        {
            this.Topic = topic;
            this.Partition = partition;
            this.Offset = offset;
            this.Key = key;
            this.Value = value;
            this.UnixCreatedTimeMS = unixCreatedTimeMS;
        }

        public string Topic { get; private set; }

        public int Partition { get; private set; }

        public long Offset { get; private set; }

        public string Key { get; private set; }

        public string Value { get; private set; }

        public long UnixCreatedTimeMS { get; private set; }
    }
}
